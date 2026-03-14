package com.amazonaws.services.msf;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.config.KinesisSourceConfigOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

public class KinesisToS3Job {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties();
        
        env.enableCheckpointing(10000);
        env.setParallelism(1);

        System.out.println("Starting Kinesis to S3 job...");
        
        KinesisStreamsSource<String> source = createKinesisSource(props.get("KinesisSource"));
        
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kinesis Source"
        ).returns(Types.STRING);
        
        stream = stream.map(msg -> msg + "\n");

        FileSink<String> sink = createS3Sink(props.get("S3Sink"));
        stream.sinkTo(sink);
        
        System.out.println("Job configured, starting execution...");
        env.execute("Kinesis to S3");
    }

    private static KinesisStreamsSource<String> createKinesisSource(Properties props) {
        String streamName = props.getProperty("stream.name");
        String region = props.getProperty("aws.region");
        String accountId = props.getProperty("aws.account.id");
        String startPosition = props.getProperty("start.position", "LATEST");
        
        System.out.println("=== Kinesis Source Configuration ===");
        System.out.println("Stream: " + streamName);
        System.out.println("Region: " + region);
        System.out.println("Account: " + accountId);
        System.out.println("Start Position: " + startPosition);
        
        Configuration sourceConfig = new Configuration();
        
        if (startPosition.equals("AT_TIMESTAMP")) {
            String timestamp = props.getProperty("start.timestamp");
            System.out.println("Start Timestamp: " + timestamp);
            sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, 
                    KinesisSourceConfigOptions.InitialPosition.AT_TIMESTAMP);
            sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_TIMESTAMP, timestamp);
        } else if (startPosition.equals("TRIM_HORIZON")) {
            sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, 
                    KinesisSourceConfigOptions.InitialPosition.TRIM_HORIZON);
        } else {
            sourceConfig.set(KinesisSourceConfigOptions.STREAM_INITIAL_POSITION, 
                    KinesisSourceConfigOptions.InitialPosition.LATEST);
        }
        
        System.out.println("====================================");

        return KinesisStreamsSource.<String>builder()
                .setStreamArn("arn:aws:kinesis:" + region + ":" + accountId + ":stream/" + streamName)
                .setSourceConfig(sourceConfig)
                .setDeserializationSchema(new SimpleStringSchema())
                .build();
    }

    private static FileSink<String> createS3Sink(Properties props) {
        String outputPath = props.getProperty("output.path");
        
        System.out.println("=== S3 Sink Configuration ===");
        System.out.println("Output Path: " + outputPath);
        System.out.println("=============================");
        
        return FileSink
                .<String>forRowFormat(new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
                .withBucketAssigner(new DateTimeBucketAssigner<>("'date='yyyy-MM-dd"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofMinutes(2))
                                .withInactivityInterval(Duration.ofSeconds(30))
                                .withMaxPartSize(10 * 1024 * 1024L)
                                .build()
                )
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("kinesis-data")
                                .withPartSuffix(".json")
                                .build()
                )
                .build();
    }
}
