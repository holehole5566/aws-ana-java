package com.amazonaws.services.msf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Real-time CDC pipeline from MySQL to Elasticsearch.
 */
public class MysqlToElasticsearchJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(MysqlToElasticsearchJob.class);

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
        Properties appConfig = applicationProperties.get("ApplicationConfig");
        
        int parallelism = Integer.parseInt(appConfig.getProperty("parallelism", "4"));
        env.setParallelism(parallelism);
        
        long checkpointInterval = Long.parseLong(appConfig.getProperty("checkpoint.interval.ms", "5000"));
        env.enableCheckpointing(checkpointInterval);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        boolean operatorChaining = Boolean.parseBoolean(appConfig.getProperty("operator.chaining", "false"));
        tEnv.getConfig().set("pipeline.operator-chaining", String.valueOf(operatorChaining));

        LOG.info("Flink environment initialized with parallelism: {}", parallelism);

        createMysqlCdcSourceTable(tEnv, applicationProperties.get("MysqlSource"));
        createElasticsearchSinkTable(tEnv, applicationProperties.get("ElasticsearchSink"));
        executeEtlPipeline(tEnv, parallelism);
        
        LOG.info("MySQL to Elasticsearch streaming job submitted");
    }

    private static void createMysqlCdcSourceTable(StreamTableEnvironment tEnv, Properties mysqlProperties) {
        String hostname = mysqlProperties.getProperty("hostname");
        String port = mysqlProperties.getProperty("port");
        String username = mysqlProperties.getProperty("username");
        String password = mysqlProperties.getProperty("password");
        String databaseName = mysqlProperties.getProperty("database.name");
        String tableName = mysqlProperties.getProperty("table.name");
        String serverTimezone = mysqlProperties.getProperty("server.timezone", "UTC");
        String serverId = mysqlProperties.getProperty("server.id", "1000000-2000000");
        String chunkSize = mysqlProperties.getProperty("chunk.size", "2000");
        String fetchSize = mysqlProperties.getProperty("fetch.size", "2000");
        
        LOG.info("Creating MySQL CDC source table for: {}.{}", databaseName, tableName);
        
        String sql = String.format(
                "CREATE TABLE t_user_asset_record_src (\n" +
                "    f_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_order_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_seq_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_user_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_symbol CHAR(16) NOT NULL,\n" +
                "    f_type SMALLINT NOT NULL,\n" +
                "    f_action SMALLINT NOT NULL,\n" +
                "    f_amount DECIMAL(36,18) NOT NULL,\n" +
                "    f_available DECIMAL(36,18) NOT NULL,\n" +
                "    f_hold DECIMAL(36,18) NOT NULL,\n" +
                "    f_intra_delta DECIMAL(36,18) NOT NULL,\n" +
                "    f_fee DECIMAL(36,18) NOT NULL,\n" +
                "    f_version DECIMAL(20, 0) NOT NULL,\n" +
                "    f_created_at DECIMAL(20, 0) NOT NULL,\n" +
                "    PRIMARY KEY(f_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'hostname' = '%s',\n" +
                "    'port' = '%s',\n" +
                "    'username' = '%s',\n" +
                "    'password' = '%s',\n" +
                "    'database-name' = '%s',\n" +
                "    'table-name' = '%s',\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'server-time-zone' = '%s',\n" +
                "    'server-id' = '%s',\n" +
                "    'debezium.skipped.operations' = 'd',\n" +
                "    'chunk-key.even-distribution.factor.lower-bound' = '1',\n" +
                "    'chunk-key.even-distribution.factor.upper-bound' = '1',\n" +
                "    'scan.incremental.snapshot.enabled' = 'true',\n" +
                "    'scan.incremental.snapshot.chunk.size' = '%s',\n" +
                "    'scan.snapshot.fetch.size' = '%s'\n" +
                ")",
                hostname, port, username, password, databaseName, tableName,
                serverTimezone, serverId, chunkSize, fetchSize
        );
        
        tEnv.executeSql(sql);
        LOG.info("MySQL CDC source table created successfully");
    }

    private static void createElasticsearchSinkTable(StreamTableEnvironment tEnv, Properties esProperties) {
        String hosts = esProperties.getProperty("hosts");
        String username = esProperties.getProperty("username");
        String password = esProperties.getProperty("password");
        String index = esProperties.getProperty("index");
        String bulkFlushMaxActions = esProperties.getProperty("bulk.flush.max.actions", "1000");
        String bulkFlushInterval = esProperties.getProperty("bulk.flush.interval", "1s");
        String bulkFlushMaxSize = esProperties.getProperty("bulk.flush.max.size", "5mb");
        String bulkFlushMaxRetries = esProperties.getProperty("bulk.flush.max.retries", "20");
        String bulkFlushBackoffDelay = esProperties.getProperty("bulk.flush.backoff.delay", "500ms");
        
        LOG.info("Creating Elasticsearch sink table for index: {}", index);
        
        String sql = String.format(
                "CREATE TABLE t_user_asset_record_sink (\n" +
                "    f_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_order_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_seq_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_user_id DECIMAL(20, 0) NOT NULL,\n" +
                "    f_symbol CHAR(16) NOT NULL,\n" +
                "    f_type SMALLINT NOT NULL,\n" +
                "    f_action SMALLINT NOT NULL,\n" +
                "    f_amount DECIMAL(36,18) NOT NULL,\n" +
                "    f_available DECIMAL(36,18) NOT NULL,\n" +
                "    f_hold DECIMAL(36,18) NOT NULL,\n" +
                "    f_intra_delta DECIMAL(36,18) NOT NULL,\n" +
                "    f_fee DECIMAL(36,18) NOT NULL,\n" +
                "    f_version DECIMAL(20, 0) NOT NULL,\n" +
                "    f_created_at DECIMAL(20, 0) NOT NULL,\n" +
                "    PRIMARY KEY(f_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'elasticsearch-7',\n" +
                "    'hosts' = '%s',\n" +
                "    'username' = '%s',\n" +
                "    'password' = '%s',\n" +
                "    'index' = '%s',\n" +
                "    'format' = 'json',\n" +
                "    'json.timestamp-format.standard' = 'ISO-8601',\n" +
                "    'sink.bulk-flush.max-actions' = '%s',\n" +
                "    'sink.bulk-flush.interval' = '%s',\n" +
                "    'sink.bulk-flush.max-size' = '%s',\n" +
                "    'sink.bulk-flush.backoff.strategy' = 'EXPONENTIAL',\n" +
                "    'sink.bulk-flush.backoff.max-retries' = '%s',\n" +
                "    'sink.bulk-flush.backoff.delay' = '%s',\n" +
                "    'sink.flush-on-checkpoint' = 'true'\n" +
                ")",
                hosts, username, password, index, bulkFlushMaxActions, bulkFlushInterval,
                bulkFlushMaxSize, bulkFlushMaxRetries, bulkFlushBackoffDelay
        );
        
        tEnv.executeSql(sql);
        LOG.info("Elasticsearch sink table created successfully");
    }

    private static void executeEtlPipeline(StreamTableEnvironment tEnv, int parallelism) {
        LOG.info("Executing ETL pipeline with parallelism: {}", parallelism);
        
        String sql = String.format(
                "INSERT INTO t_user_asset_record_sink \n" +
                "SELECT f_id, f_order_id, f_seq_id, f_user_id, f_symbol, f_type, f_action, " +
                "f_amount, f_available, f_hold, f_intra_delta, f_fee, f_version, f_created_at \n" +
                "FROM (\n" +
                "  SELECT f_id, f_order_id, f_seq_id, f_user_id, f_symbol, f_type, f_action, " +
                "f_amount, f_available, f_hold, f_intra_delta, f_fee, f_version, f_created_at,\n" +
                "         MOD(f_id, %d) as partition_key\n" +
                "  FROM t_user_asset_record_src\n" +
                ") t",
                parallelism
        );
        
        tEnv.executeSql(sql);
    }
}
