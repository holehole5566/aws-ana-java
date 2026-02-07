package com.neptune.loadtest;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class NeptuneLoadTest {
    // Configuration loaded from properties file
    private static String NEPTUNE_ENDPOINT;
    private static int NEPTUNE_PORT;
    private static String AWS_REGION;
    private static boolean ENABLE_IAM;
    private static boolean ENABLE_SSL;
    private static int MAX_WAIT_FOR_CONNECTION;
    private static int MAX_IN_PROCESS_PER_CONNECTION;
    private static int THREAD_COUNT;
    private static int DURATION_SECONDS;
    private static int TARGET_QPS;
    
    private static final List<QueryResult> queryResults = Collections.synchronizedList(new ArrayList<>());
    private static final AtomicInteger queryCount = new AtomicInteger(0);
    private static final AtomicInteger errorCount = new AtomicInteger(0);
    private static final Random random = new Random();

    /**
     * Load configuration from properties file
     */
    private static void loadConfiguration() {
        Properties properties = new Properties();
        String configFile = "neptune-loadtest-local.properties";
        
        try (InputStream input = NeptuneLoadTest.class.getClassLoader().getResourceAsStream(configFile)) {
            if (input == null) {
                System.out.println("Unable to find " + configFile + ", trying neptune-loadtest.properties");
                configFile = "neptune-loadtest.properties";
                try (InputStream fallbackInput = NeptuneLoadTest.class.getClassLoader().getResourceAsStream(configFile)) {
                    if (fallbackInput == null) {
                        throw new IOException("Unable to find configuration file");
                    }
                    properties.load(fallbackInput);
                }
            } else {
                properties.load(input);
            }
            
            NEPTUNE_ENDPOINT = properties.getProperty("neptune.endpoint");
            NEPTUNE_PORT = Integer.parseInt(properties.getProperty("neptune.port", "8182"));
            AWS_REGION = properties.getProperty("neptune.region", "us-east-1");
            ENABLE_IAM = Boolean.parseBoolean(properties.getProperty("neptune.enable.iam", "false"));
            ENABLE_SSL = Boolean.parseBoolean(properties.getProperty("neptune.enable.ssl", "true"));
            MAX_WAIT_FOR_CONNECTION = Integer.parseInt(properties.getProperty("neptune.max.wait.connection", "30000"));
            MAX_IN_PROCESS_PER_CONNECTION = Integer.parseInt(properties.getProperty("neptune.max.in.process.per.connection", "32"));
            THREAD_COUNT = Integer.parseInt(properties.getProperty("loadtest.thread.count", "20"));
            DURATION_SECONDS = Integer.parseInt(properties.getProperty("loadtest.duration.seconds", "1200"));
            TARGET_QPS = Integer.parseInt(properties.getProperty("loadtest.target.qps", "1500"));
            
            System.out.println("Configuration loaded from " + configFile);
            System.out.println("Neptune endpoint: " + NEPTUNE_ENDPOINT);
            System.out.println("Thread count: " + THREAD_COUNT);
            System.out.println("Target QPS: " + TARGET_QPS);
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to load configuration: " + e.getMessage(), e);
        }
    }

    static class QueryResult {
        String clientRequestId;
        String queryType;
        String userId;
        double latencyMs;
        int resultCount;
        String timestamp;
        String status;
        int threadId;

        public QueryResult(String clientRequestId, String queryType, String userId, 
                          double latencyMs, int resultCount, String timestamp, String status, int threadId) {
            this.clientRequestId = clientRequestId;
            this.queryType = queryType;
            this.userId = userId;
            this.latencyMs = latencyMs;
            this.resultCount = resultCount;
            this.timestamp = timestamp;
            this.status = status;
            this.threadId = threadId;
        }
    }

    static class WorkerThread implements Runnable {
        private final int threadId;
        private final int qpsPerThread;
        private final int durationSeconds;
        private final long timeFilter;
        private final String[] queryTypes = {"fast", "medium", "slow"};
        private Cluster cluster;
        private Client client;

        public WorkerThread(int threadId, int qpsPerThread, int durationSeconds, long timeFilter) {
            this.threadId = threadId;
            this.qpsPerThread = qpsPerThread;
            this.durationSeconds = durationSeconds;
            this.timeFilter = timeFilter;
        }

        private void initializeCluster() {
            Cluster.Builder builder = Cluster.build()
                    .addContactPoint(NEPTUNE_ENDPOINT)
                    .port(NEPTUNE_PORT)
                    .maxWaitForConnection(MAX_WAIT_FOR_CONNECTION)
                    .maxInProcessPerConnection(MAX_IN_PROCESS_PER_CONNECTION)
                    .enableSsl(ENABLE_SSL);

            if (ENABLE_IAM) {
                try {
                    NeptuneNettyHttpSigV4Signer sigV4Signer = new NeptuneNettyHttpSigV4Signer(
                        AWS_REGION, 
                        new DefaultAWSCredentialsProviderChain()
                    );
                    builder.handshakeInterceptor(r -> {
                        try {
                            sigV4Signer.signRequest(r);
                            return r;
                        } catch (NeptuneSigV4SignerException e) {
                            throw new RuntimeException("SigV4 signing failed", e);
                        }
                    });
                } catch (Exception e) {
                    throw new RuntimeException("Failed to configure SigV4 signing", e);
                }
            }

            cluster = builder.create();
            client = cluster.connect();
            System.out.printf("Thread %d: Cluster initialized%n", threadId);
        }

        private void executeQuery(String queryType) {
            String clientRequestId = UUID.randomUUID().toString();
            String userId = String.format("user-%06d", random.nextInt(5) + 1);
            String gremlinQuery = buildGremlinQuery(queryType, userId, timeFilter);
            
            long startTime = System.currentTimeMillis();
            
            try {
                ResultSet results = client.submit(gremlinQuery);
                List<Result> resultList = results.all().get(30, TimeUnit.SECONDS);
                
                double latency = System.currentTimeMillis() - startTime;
                int resultCount = resultList.size();
                
                queryResults.add(new QueryResult(
                    clientRequestId, queryType, userId, latency, resultCount,
                    LocalDateTime.now().toString(), "success", threadId
                ));
                queryCount.incrementAndGet();
                
            } catch (TimeoutException e) {
                double latency = System.currentTimeMillis() - startTime;
                queryResults.add(new QueryResult(
                    clientRequestId, queryType, userId, latency, 0,
                    LocalDateTime.now().toString(), "timeout: " + e.getMessage(), threadId
                ));
                errorCount.incrementAndGet();
            } catch (Exception e) {
                double latency = System.currentTimeMillis() - startTime;
                queryResults.add(new QueryResult(
                    clientRequestId, queryType, userId, latency, 0,
                    LocalDateTime.now().toString(), "error: " + e.getMessage(), threadId
                ));
                errorCount.incrementAndGet();
            }
        }

        @Override
        public void run() {
            try {
                initializeCluster();
                
                ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
                long intervalMs = 1000 / qpsPerThread;
                
                for (int i = 0; i < durationSeconds * qpsPerThread; i++) {
                    String queryType = queryTypes[random.nextInt(queryTypes.length)];
                    scheduler.schedule(() -> executeQuery(queryType), i * intervalMs, TimeUnit.MILLISECONDS);
                }
                
                scheduler.shutdown();
                scheduler.awaitTermination(durationSeconds + 10, TimeUnit.SECONDS);
                
            } catch (Exception e) {
                System.err.printf("Thread %d error: %s%n", threadId, e.getMessage());
            } finally {
                if (client != null) client.close();
                if (cluster != null) cluster.close();
                System.out.printf("Thread %d: Connections closed%n", threadId);
            }
        }
    }

    private static String buildGremlinQuery(String queryType, String userId, long timeFilter) {
        switch (queryType) {
            case "fast":
                return String.format(
                    "g.V('%s').as('center').union(" +
                    "__.out('uses_device').has('gmt_occur', gte(%d))," +
                    "__.out('uses_ip').has('gmt_occur', gte(%d))," +
                    "__.out('uses_email').has('gmt_occur', gte(%d))," +
                    "__.out('uses_oneid').has('gmt_occur', gte(%d))," +
                    "__.out('recommends').has('gmt_occur', gte(%d))" +
                    ").toList()",
                    userId, timeFilter, timeFilter, timeFilter, timeFilter, timeFilter
                );
            case "medium":
                return String.format(
                    "g.V('%s').as('center').union(" +
                    "__.out('uses_device').has('gmt_occur', gte(%d))," +
                    "__.out('uses_ip').has('gmt_occur', gte(%d))," +
                    "__.out('deposits_from').has('gmt_occur', gte(%d))," +
                    "__.out('uses_email').has('gmt_occur', gte(%d))," +
                    "__.out('recommends').has('gmt_occur', gte(%d))" +
                    ").where(__.out('deposits_from').has('amount', gte(1000))).toList()",
                    userId, timeFilter, timeFilter, timeFilter, timeFilter, timeFilter
                );
            default: // slow
                return String.format(
                    "g.V('%s').as('center').union(" +
                    "__.out('uses_device').has('gmt_occur', gte(%d))," +
                    "__.out('uses_ip').has('gmt_occur', gte(%d))," +
                    "__.out('withdraws_to').has('gmt_occur', gte(%d))," +
                    "__.out('transfers_to').has('gmt_occur', gte(%d))," +
                    "__.out('recommends').has('gmt_occur', gte(%d))" +
                    ").where(__.out('withdraws_to').has('amount', gte(500))).toList()",
                    userId, timeFilter, timeFilter, timeFilter, timeFilter, timeFilter
                );
        }
    }

    private static void exportResultsToCsv(String filename) {
        try (FileWriter writer = new FileWriter(filename);
             CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                     .withHeader("client_request_id", "query_type", "user_id", 
                               "latency_ms", "result_count", "timestamp", "status", "thread_id"))) {
            
            for (QueryResult result : queryResults) {
                csvPrinter.printRecord(
                    result.clientRequestId, result.queryType, result.userId,
                    result.latencyMs, result.resultCount, result.timestamp, result.status, result.threadId
                );
            }
            System.out.println("Results exported to " + filename);
        } catch (IOException e) {
            System.err.println("Error exporting CSV: " + e.getMessage());
        }
    }

    private static void loadTest(int durationSeconds, int targetQps) throws InterruptedException {
        queryResults.clear();
        queryCount.set(0);
        errorCount.set(0);
        
        int qpsPerThread = targetQps / THREAD_COUNT;
        System.out.printf("Starting load test: %d threads, %d QPS per thread, total %d QPS for %d seconds%n", 
                         THREAD_COUNT, qpsPerThread, targetQps, durationSeconds);
        
        long timeFilter = System.currentTimeMillis() / 1000 - 10;
        
        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        List<Future<?>> futures = new ArrayList<>();
        
        for (int i = 0; i < THREAD_COUNT; i++) {
            WorkerThread worker = new WorkerThread(i, qpsPerThread, durationSeconds, timeFilter);
            futures.add(executor.submit(worker));
        }
        
        executor.shutdown();
        executor.awaitTermination(durationSeconds + 30, TimeUnit.SECONDS);
        
        // Wait for all threads to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                System.err.println("Thread execution error: " + e.getMessage());
            }
        }
        
        Thread.sleep(2000);
        
        // Export results
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String filename = "neptune_query_results_" + timestamp + ".csv";
        exportResultsToCsv(filename);
        
        // Print summary
        List<Double> latencies = queryResults.stream()
                .filter(r -> "success".equals(r.status))
                .map(r -> r.latencyMs)
                .collect(Collectors.toList());
        
        System.out.println("\n=== Load Test Results ===");
        System.out.printf("Duration: %ds%n", durationSeconds);
        System.out.printf("Target QPS: %d%n", targetQps);
        System.out.printf("Actual QPS: %.2f%n", queryCount.get() / (double) durationSeconds);
        System.out.printf("Total queries: %d%n", queryCount.get());
        System.out.printf("Errors: %d%n", errorCount.get());
        
        if (queryCount.get() > 0) {
            System.out.printf("Success rate: %.2f%%%n", 
                ((queryCount.get() - errorCount.get()) / (double) queryCount.get() * 100));
        }
        
        if (!latencies.isEmpty()) {
            Collections.sort(latencies);
            DoubleSummaryStatistics stats = latencies.stream()
                    .mapToDouble(Double::doubleValue)
                    .summaryStatistics();
            
            System.out.println("Response times (ms):");
            System.out.printf("  Min: %.2f%n", stats.getMin());
            System.out.printf("  Max: %.2f%n", stats.getMax());
            System.out.printf("  Avg: %.2f%n", stats.getAverage());
            System.out.printf("  P50: %.2f%n", latencies.get((int) (latencies.size() * 0.5)));
            System.out.printf("  P95: %.2f%n", latencies.get((int) (latencies.size() * 0.95)));
            System.out.printf("  P99: %.2f%n", latencies.get((int) (latencies.size() * 0.99)));
        }
    }

    public static void main(String[] args) {
        try {
            // Load configuration from properties file
            loadConfiguration();
            
            // Run load test with configured parameters
            loadTest(DURATION_SECONDS, TARGET_QPS);
        } catch (InterruptedException e) {
            System.out.println("\nTest interrupted by user");
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
