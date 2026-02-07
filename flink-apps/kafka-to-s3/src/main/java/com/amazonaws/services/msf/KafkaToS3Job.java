package com.amazonaws.services.msf;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

/**
 * A Flink application that reads data from a Kafka topic and writes it to Amazon S3.
 * 
 * <p>The application can run both locally and on Amazon Managed Service for Apache Flink.
 * Configuration is loaded from application properties based on the execution environment.
 */
public class KafkaToS3Job {
    
    private static final Logger LOG = LoggerFactory.getLogger(KafkaToS3Job.class);

    /**
     * Main method to execute the Flink streaming job.
     */
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Load application properties
        Map<String, Properties> applicationProperties = loadApplicationProperties(env);
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(180000); // 3 minutes
        
        LOG.info("Flink environment initialized successfully");

        // Create Kafka source
        KafkaSource<String> kafkaSource = createKafkaSource(applicationProperties.get("KafkaSource"));
        
        // Read from Kafka
        DataStream<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // Add newline to each JSON message for better readability
        DataStream<String> formattedStream = kafkaStream.map(message -> message + "\n");

        // Create S3 sink
        FileSink<String> s3Sink = createS3Sink(applicationProperties.get("S3Sink"));

        // Write to S3
        formattedStream.sinkTo(s3Sink);

        LOG.info("Starting Kafka to S3 streaming job");
        env.execute("Kafka to S3 Streaming Job");
    }

    /**
     * Creates a Kafka source with the provided configuration.
     */
    private static KafkaSource<String> createKafkaSource(Properties kafkaProperties) {
        String bootstrapServers = kafkaProperties.getProperty("bootstrap.servers");
        String topic = kafkaProperties.getProperty("topic");
        String groupId = kafkaProperties.getProperty("group.id");
        
        LOG.info("Creating Kafka source for topic: {}", topic);
        
        // Build Kafka properties for the connector
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", bootstrapServers);
        kafkaProps.setProperty("group.id", groupId);
        
        // Add security configuration if present
        if (kafkaProperties.containsKey("security.protocol")) {
            kafkaProps.setProperty("security.protocol", kafkaProperties.getProperty("security.protocol"));
            kafkaProps.setProperty("sasl.mechanism", kafkaProperties.getProperty("sasl.mechanism"));
            kafkaProps.setProperty("sasl.jaas.config", kafkaProperties.getProperty("sasl.jaas.config"));
            LOG.info("Kafka security configured with protocol: {}", kafkaProperties.getProperty("security.protocol"));
        }
        
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();
    }

    /**
     * Creates an S3 FileSink with the provided configuration.
     */
    private static FileSink<String> createS3Sink(Properties s3Properties) {
        String outputPath = s3Properties.getProperty("output.path");
        String filePrefix = s3Properties.getProperty("file.prefix", "kafka-data");
        String fileSuffix = s3Properties.getProperty("file.suffix", ".json");
        
        long rolloverInterval = Long.parseLong(s3Properties.getProperty("rollover.interval.minutes", "2"));
        long inactivityInterval = Long.parseLong(s3Properties.getProperty("inactivity.interval.seconds", "30"));
        long maxPartSize = Long.parseLong(s3Properties.getProperty("max.part.size.mb", "10"));
        
        LOG.info("Creating S3 sink with output path: {}", outputPath);
        
        return FileSink
                .<String>forRowFormat(
                        new Path(outputPath),
                        new SimpleStringEncoder<>("UTF-8")
                )
                .withBucketAssigner(new DateTimeBucketAssigner<>("'date='yyyy-MM-dd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(rolloverInterval))
                                .withInactivityInterval(Duration.ofSeconds(inactivityInterval))
                                .withMaxPartSize(maxPartSize * 1024 * 1024L)
                                .build()
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix(filePrefix)
                                .withPartSuffix(fileSuffix)
                                .build()
                )
                .build();
    }

    /**
     * Loads application properties from the appropriate source based on the execution environment.
     * When running locally, properties are loaded from a JSON file.
     * When running on Managed Service for Apache Flink, properties are loaded from the runtime.
     */
    private static Map<String, Properties> loadApplicationProperties(StreamExecutionEnvironment env) throws IOException {
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }
}
