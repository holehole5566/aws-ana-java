package com.amazonaws.services.msf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Streams data from Amazon MSK to Apache Iceberg tables.
 */
public class MskToIcebergJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(MskToIcebergJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        LOG.info("Flink environment initialized successfully");

        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        
        createMskSourceTable(tEnv, applicationProperties.get("MskSource"));
        createIcebergSinkTable(tEnv, applicationProperties.get("IcebergSink"));
        executeEtlPipeline(tEnv);
        
        LOG.info("MSK to Iceberg streaming job submitted");
    }

    private static void createMskSourceTable(StreamTableEnvironment tEnv, Properties mskProperties) {
        String bootstrapServers = mskProperties.getProperty("bootstrap.servers");
        String topic = mskProperties.getProperty("topic");
        String groupId = mskProperties.getProperty("group.id");
        String securityProtocol = mskProperties.getProperty("security.protocol", "");
        String saslMechanism = mskProperties.getProperty("sasl.mechanism", "");
        String saslJaasConfig = mskProperties.getProperty("sasl.jaas.config", "");
        
        LOG.info("Creating MSK source table for topic: {}", topic);
        
        StringBuilder sql = new StringBuilder(
                "CREATE TEMPORARY TABLE msk_source (\n" +
                "  message STRING,\n" +
                "  proc_time AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '" + topic + "',\n" +
                "  'properties.bootstrap.servers' = '" + bootstrapServers + "',\n" +
                "  'properties.group.id' = '" + groupId + "',\n" +
                "  'format' = 'raw',\n" +
                "  'scan.startup.mode' = 'earliest-offset'");
        
        if (!securityProtocol.isEmpty()) {
            sql.append(",\n  'properties.security.protocol' = '").append(securityProtocol).append("'");
            sql.append(",\n  'properties.sasl.mechanism' = '").append(saslMechanism).append("'");
            sql.append(",\n  'properties.sasl.jaas.config' = '").append(saslJaasConfig).append("'");
        }
        
        sql.append("\n)");
        
        tEnv.executeSql(sql.toString());
        LOG.info("MSK source table created successfully");
    }

    private static void createIcebergSinkTable(StreamTableEnvironment tEnv, Properties icebergProperties) {
        String warehousePath = icebergProperties.getProperty("warehouse.path");
        String databaseName = icebergProperties.getProperty("database.name");
        String tableName = icebergProperties.getProperty("table.name");
        String region = icebergProperties.getProperty("aws.region");
        
        LOG.info("Creating Iceberg sink table: {}.{}", databaseName, tableName);
        
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS iceberg_sink (\n" +
                "  payload STRING,\n" +
                "  ts BIGINT,\n" +
                "  processing_timestamp TIMESTAMP(3),\n" +
                "  partition_date STRING\n" +
                ") PARTITIONED BY (partition_date)\n" +
                "WITH (\n" +
                "  'connector' = 'iceberg',\n" +
                "  'catalog-name' = 'glue_catalog',\n" +
                "  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',\n" +
                "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                "  'client.region' = '%s',\n" +
                "  'write.format.default' = 'parquet',\n" +
                "  'warehouse' = '%s',\n" +
                "  'database-name' = '%s',\n" +
                "  'table-name' = '%s'\n" +
                ")",
                region, warehousePath, databaseName, tableName
        );
        
        tEnv.executeSql(sql);
        LOG.info("Iceberg sink table created successfully");
    }

    private static void executeEtlPipeline(StreamTableEnvironment tEnv) {
        LOG.info("Executing ETL pipeline");
        
        String sql = 
                "INSERT INTO iceberg_sink\n" +
                "SELECT\n" +
                "  message as payload,\n" +
                "  CAST(JSON_VALUE(message, '$.timestamp') AS BIGINT) as ts,\n" +
                "  CAST(proc_time AS TIMESTAMP(3)) AS processing_timestamp,\n" +
                "  DATE_FORMAT(CAST(proc_time AS TIMESTAMP(3)), 'yyyy-MM-dd') AS partition_date\n" +
                "FROM msk_source";
        
        tEnv.executeSql(sql);
    }
}
