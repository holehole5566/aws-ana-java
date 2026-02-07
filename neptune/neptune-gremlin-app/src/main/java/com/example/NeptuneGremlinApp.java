package com.example;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.neptune.auth.NeptuneNettyHttpSigV4Signer;
import com.amazonaws.neptune.auth.NeptuneSigV4SignerException;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class NeptuneGremlinApp {
    
    private static Properties loadConfig() throws IOException {
        Properties props = new Properties();
        // Try local config first, fallback to default
        try (InputStream input = NeptuneGremlinApp.class.getClassLoader()
                .getResourceAsStream("neptune-config-local.properties")) {
            if (input != null) {
                props.load(input);
                System.out.println("✓ Loaded local configuration");
                return props;
            }
        }
        
        try (InputStream input = NeptuneGremlinApp.class.getClassLoader()
                .getResourceAsStream("neptune-config.properties")) {
            if (input != null) {
                props.load(input);
                System.out.println("✓ Loaded default configuration");
                return props;
            }
        }
        
        throw new IOException("Configuration file not found");
    }
    
    public static void main(String[] args) {
        System.out.println("=== Neptune Gremlin Test ===");
        
        try {
            Properties config = loadConfig();
            
            String endpoint = config.getProperty("neptune.endpoint");
            int port = Integer.parseInt(config.getProperty("neptune.port"));
            String region = config.getProperty("neptune.region");
            boolean enableSsl = Boolean.parseBoolean(config.getProperty("neptune.enable.ssl"));
            
            String accessKey = config.getProperty("aws.access.key.id");
            String secretKey = config.getProperty("aws.secret.access.key");
            
            System.setProperty("AWS_JAVA_V1_DISABLE_DEPRECATION_ANNOUNCEMENT", "true");
            
            // Set credentials if provided
            if (accessKey != null && !accessKey.isEmpty() && !accessKey.startsWith("your-")) {
                System.setProperty("aws.accessKeyId", accessKey);
                System.setProperty("aws.secretKey", secretKey);
            }

            Cluster cluster = Cluster.build(endpoint)
                    .port(port)
                    .enableSsl(enableSsl)
                    .requestInterceptor(r -> {
                        try {
                            NeptuneNettyHttpSigV4Signer sigV4Signer =
                                new NeptuneNettyHttpSigV4Signer(region,
                                    DefaultAWSCredentialsProviderChain.getInstance());
                            sigV4Signer.signRequest(r);
                        } catch (NeptuneSigV4SignerException e) {
                            throw new RuntimeException("Exception occurred while signing the request", e);
                        }
                        return r;
                    })
                    .create();
            
            try {
                Client client = cluster.connect();
                System.out.println("✓ Connected to Neptune cluster: " + endpoint);
                
                testExistingData(client);
                testAddVertices(client);
                testQueryNewData(client);
                cleanupTestData(client);
                
            } catch (Exception e) {
                System.err.println("✗ Error: " + e.getMessage());
                e.printStackTrace();
            } finally {
                System.out.println("\n=== Closing connection ===");
                cluster.close();
                System.out.println("✓ Connection closed");
            }
        } catch (IOException e) {
            System.err.println("✗ Failed to load configuration: " + e.getMessage());
        }
    }
    
    // 測試現有資料
    private static void testExistingData(Client client) throws Exception {
        System.out.println("查看圖形統計資訊...");
        
        // 頂點總數
        ResultSet vertexCount = client.submit("g.V().count()");
        List<Result> vResults = vertexCount.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 總頂點數: " + vResults.get(0).getObject());
        
        // 邊總數
        ResultSet edgeCount = client.submit("g.E().count()");
        List<Result> eResults = edgeCount.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 總邊數: " + eResults.get(0).getObject());
        
        // 查看前3個頂點
        ResultSet sampleVertices = client.submit("g.V().limit(3).valueMap()");
        List<Result> sampleResults = sampleVertices.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 前3個頂點範例:");
        for (int i = 0; i < sampleResults.size(); i++) {
            System.out.println("  頂點 " + (i+1) + ": " + sampleResults.get(i).getObject());
        }
    }
    
    // 測試新增頂點
    private static void testAddVertices(Client client) throws Exception {
        System.out.println("新增測試頂點...");
        
        // 新增機場頂點
        String addAirport1 = "g.addV('airport')" +
                ".property('code', 'TEST1')" +
                ".property('name', 'Test Airport 1')" +
                ".property('city', 'Test City 1')" +
                ".property('country', 'Test Country')";
        
        ResultSet result1 = client.submit(addAirport1);
        List<Result> results1 = result1.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 新增機場 TEST1: " + results1.get(0).getObject());
        
        // 新增第二個機場頂點
        String addAirport2 = "g.addV('airport')" +
                ".property('code', 'TEST2')" +
                ".property('name', 'Test Airport 2')" +
                ".property('city', 'Test City 2')" +
                ".property('country', 'Test Country')";
        
        ResultSet result2 = client.submit(addAirport2);
        List<Result> results2 = result2.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 新增機場 TEST2: " + results2.get(0).getObject());
        
        // 新增航空公司頂點
        String addAirline = "g.addV('airline')" +
                ".property('code', 'TESTAIR')" +
                ".property('name', 'Test Airline')" +
                ".property('country', 'Test Country')";
        
        ResultSet result3 = client.submit(addAirline);
        List<Result> results3 = result3.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 新增航空公司 TESTAIR: " + results3.get(0).getObject());
    }
    
    // 測試新增邊
    private static void testAddEdges(Client client) throws Exception {

        System.out.println("新增測試邊...");
        
        // 新增航線 (機場之間的連接)
        String addRoute = "g.V().has('code', 'TEST1')" +
                ".addE('route')" +
                ".to(g.V().has('code', 'TEST2'))" +
                ".property('distance', 1000)" +
                ".property('airline', 'TESTAIR')";
        
        ResultSet routeResult = client.submit(addRoute);
        List<Result> routeResults = routeResult.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 新增航線 TEST1 -> TEST2: " + routeResults.get(0).getObject());
        
        // 新增航空公司服務機場的關係
        String addService1 = "g.V().has('airline', 'code', 'TESTAIR')" +
                ".addE('serves')" +
                ".to(g.V().has('airport', 'code', 'TEST1'))" +
                ".property('service_type', 'domestic')";
        
        try {
            ResultSet serviceResult1 = client.submit(addService1);
            List<Result> serviceResults1 = serviceResult1.all().get(10, TimeUnit.SECONDS);
            System.out.println("✓ 新增服務關係 TESTAIR -> TEST1: " + serviceResults1.get(0).getObject());
        } catch (Exception e) {
            System.out.println("⚠ 服務關係可能已存在或查詢語法需調整");
        }
        
        // 反向航線
        String addReverseRoute = "g.V().has('code', 'TEST2')" +
                ".addE('route')" +
                ".to(g.V().has('code', 'TEST1'))" +
                ".property('distance', 1000)" +
                ".property('airline', 'TESTAIR')";
        
        ResultSet reverseResult = client.submit(addReverseRoute);
        List<Result> reverseResults = reverseResult.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 新增反向航線 TEST2 -> TEST1: " + reverseResults.get(0).getObject());
    }
    
    // 測試查詢新增的資料
    private static void testQueryNewData(Client client) throws Exception {
        System.out.println("查詢新增的測試資料...");
        
        // 查詢測試機場
        ResultSet testAirports = client.submit("g.V().has('code', within('TEST1', 'TEST2')).valueMap()");
        List<Result> airportResults = testAirports.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 找到 " + airportResults.size() + " 個測試機場:");
        for (Result result : airportResults) {
            System.out.println("  " + result.getObject());
        }
        
        // 查詢測試航線
        ResultSet testRoutes = client.submit("g.E().hasLabel('route').where(outV().has('code', within('TEST1', 'TEST2'))).valueMap()");
        List<Result> routeResults = testRoutes.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 找到 " + routeResults.size() + " 條測試航線:");
        for (Result result : routeResults) {
            System.out.println("  " + result.getObject());
        }
        
        // 查詢從 TEST1 出發的所有航線
        ResultSet fromTest1 = client.submit("g.V().has('code', 'TEST1').out('route').values('code', 'name')");
        List<Result> fromTest1Results = fromTest1.all().get(10, TimeUnit.SECONDS);
        System.out.println("- 從 TEST1 可到達的機場:");
        for (Result result : fromTest1Results) {
            System.out.println("  " + result.getObject());
        }
    }
    
    // 清理測試資料 (可選)
    private static void cleanupTestData(Client client) throws Exception {
        System.out.println("清理測試資料...");
        
        // 刪除測試邊
        ResultSet deleteEdges = client.submit("g.E().where(outV().has('code', within('TEST1', 'TEST2'))).drop()");
        deleteEdges.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 已刪除測試邊");
        
        // 刪除測試頂點
        ResultSet deleteVertices = client.submit("g.V().has('code', within('TEST1', 'TEST2', 'TESTAIR')).drop()");
        deleteVertices.all().get(10, TimeUnit.SECONDS);
        System.out.println("✓ 已刪除測試頂點");
    }
}

// mvn clean compile exec:java -Dexec.mainClass="com.example.NeptuneGremlinApp"
