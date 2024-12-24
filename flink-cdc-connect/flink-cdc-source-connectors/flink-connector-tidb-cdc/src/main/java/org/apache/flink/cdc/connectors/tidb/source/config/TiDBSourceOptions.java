package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.tidb.table.utils.UriHostMapping;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class TiDBSourceOptions extends JdbcSourceOptions {

    public static final ConfigOption<Integer> TiDB_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(4000)
                    .withDescription("Integer port number of the TiDB database server.");

    public static final ConfigOption<String> PD_ADDRESSES =
            ConfigOptions.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiDB pd-server addresses");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available replication slot offsets");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of full names of tables, separated by commas, e.g. \"db1.table1, db2.table2\".");

    public static final ConfigOption<String> HOST_MAPPING =
            ConfigOptions.key("host-mapping")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "TiKV cluster's host-mapping used to configure public IP and intranet IP mapping. When the TiKV cluster is running on the intranet, you can map a set of intranet IPs to public IPs for an outside Flink cluster to access. The format is {Intranet IP1}:{Public IP1};{Intranet IP2}:{Public IP2}, e.g. 192.168.0.2:8.8.8.8;192.168.0.3:9.9.9.9.");

    public static final ConfigOption<String> JDBC_DRIVER =
            ConfigOptions.key("jdbc.driver")
                    .stringType()
                    .defaultValue("com.mysql.cj.jdbc.Driver")
                    .withDescription(
                            "JDBC driver class name, use 'com.mysql.cj.jdbc.Driver' by default.");




    public static final ConfigOption<Long> TIKV_GRPC_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC timeout in ms");

    public static final ConfigOption<Long> TIKV_GRPC_INGEST_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_INGEST_TIMEOUT)
                    .longType()
                    .noDefaultValue();

    public static final ConfigOption<Long> TIKV_GRPC_FORWARD_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_FORWARD_TIMEOUT)
                    .longType()
                    .noDefaultValue();

    public static final ConfigOption<Long> TIKV_GRPC_WARM_UP_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_WARM_UP_TIMEOUT)
                    .longType()
                    .noDefaultValue().withDescription("tikv.grpc.warm_up_timeout_in_ms");

    public static final ConfigOption<Long>  TIKV_PD_FIRST_GET_MEMBER_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_PD_FIRST_GET_MEMBER_TIMEOUT)
                    .longType()
                    .noDefaultValue().withDescription("tikv.grpc.pd_first_get_member_timeout_in_ms");

    public static final ConfigOption<Integer>  TIKV_CONN_RECYCLE_TIME =
            ConfigOptions.key(ConfigUtils.TIKV_CONN_RECYCLE_TIME)
                    .intType()
                    .noDefaultValue().withDescription("tikv.conn.recycle_time");

    public static final ConfigOption<Integer>  TIKV_INDEX_SCAN_BATCH_SIZE =
            ConfigOptions.key(ConfigUtils.TIKV_INDEX_SCAN_BATCH_SIZE)
                    .intType()
                    .noDefaultValue().withDescription("tikv.index.scan_batch_size");

    public static final ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC scan timeout in ms");

    public static final ConfigOption<Long> TIKV_TLS_RELOAD_INTERVAL =
            ConfigOptions.key(ConfigUtils.TIKV_TLS_RELOAD_INTERVAL)
                    .longType()
                    .noDefaultValue()
                    .withDescription("tikv.tls.reload_interval");

//    public static final ConfigOption<Long> TIKV_GRPC_SCAN_BATCH_SIZE =
//            ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_BATCH_SIZE)
//                    .longType()
//                    .noDefaultValue()
//                    .withDescription("tikv.grpc.scan_batch_size");

    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch get concurrency");

    public static final ConfigOption<Integer> TIKV_GRPC_MAX_FRAME_SIZE =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_MAX_FRAME_SIZE)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.grpc.max_frame_size");

    public static final ConfigOption<Integer> TIKV_GRPC_KEEPALIVE_TIME =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_KEEPALIVE_TIME)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.grpc.keepalive_time");

    public static final ConfigOption<Integer> TIKV_GRPC_KEEPALIVE_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_KEEPALIVE_TIMEOUT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.grpc.keepalive_timeout");

    public static final ConfigOption<Integer> TIKV_GRPC_IDLE_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_IDLE_TIMEOUT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.grpc.idle_timeout");

    public static final ConfigOption<Integer> TIKV_GRPC_HEALTH_CHECK_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_HEALTH_CHECK_TIMEOUT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.grpc.health_check_timeout");


    public static final ConfigOption<Integer> TIKV_HEALTH_CHECK_PERIOD_DURATION =
            ConfigOptions.key(ConfigUtils.TIKV_HEALTH_CHECK_PERIOD_DURATION)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.health_check_period_duration");




    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch scan concurrency");

    public static final ConfigOption<Integer> TIKV_INDEX_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_INDEX_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.index.scan_concurrency");

    public static final ConfigOption<Integer> TIKV_TABLE_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_TABLE_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.table.scan_concurrency");


    public static final ConfigOption<Integer> TIKV_BATCH_PUT_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_PUT_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.batch_put_concurrency");


    public static final ConfigOption<Integer> TIKV_BATCH_DELETE_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_DELETE_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.batch_delete_concurrency");


    public static final ConfigOption<Integer> TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS =
            ConfigOptions.key(ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.trigger.availability.window_in_seconds");

    public static final ConfigOption<Integer> TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE =
            ConfigOptions.key(ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.trigger.availability.error_threshold_percentage");

    public static final ConfigOption<Integer> TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD =
            ConfigOptions.key(ConfigUtils.TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.trigger.availability.request_volumn_threshold");


    public static final ConfigOption<Integer> TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS =
            ConfigOptions.key(ConfigUtils.TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.trigger.sleep_window_in_seconds");


    public static final ConfigOption<Integer> TIKV_DELETE_RANGE_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_DELETE_RANGE_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.delete_range_concurrency");

    public static final ConfigOption<Integer> TIKV_KV_CLIENT_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_KV_CLIENT_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.kv_client_concurrency");

    public static final ConfigOption<Integer> TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT =
            ConfigOptions.key(ConfigUtils.TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.trigger.attempt_request_count");

    public static final ConfigOption<Integer> TIKV_SCAN_REGIONS_LIMIT =
            ConfigOptions.key(ConfigUtils.TIKV_SCAN_REGIONS_LIMIT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.scan_regions_limit");


    public static final ConfigOption<Integer> TIKV_METRICS_PORT =
            ConfigOptions.key(ConfigUtils.TIKV_METRICS_PORT)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.metrics.port");

    public static final ConfigOption<Integer> TIKV_IMPORTER_MAX_KV_BATCH_BYTES =
            ConfigOptions.key(ConfigUtils.TIKV_IMPORTER_MAX_KV_BATCH_BYTES)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.importer.max_kv_batch_bytes");

    public static final ConfigOption<Integer> TIKV_IMPORTER_MAX_KV_BATCH_SIZE =
            ConfigOptions.key(ConfigUtils.TIKV_IMPORTER_MAX_KV_BATCH_SIZE)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.importer.max_kv_batch_size");

    public static final ConfigOption<Integer> TIKV_SCATTER_WAIT_SECONDS =
            ConfigOptions.key(ConfigUtils.TIKV_SCATTER_WAIT_SECONDS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.scatter_wait_seconds");

    public static final ConfigOption<Boolean> TIKV_SHOW_ROWID =
            ConfigOptions.key(ConfigUtils.TIKV_SHOW_ROWID)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.show_rowid");

    public static final ConfigOption<String> TIKV_DB_PREFIX =
            ConfigOptions.key(ConfigUtils.TIKV_DB_PREFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription("tikv.db_prefix");

    public static final ConfigOption<Boolean> TIKV_METRICS_ENABLE =
            ConfigOptions.key(ConfigUtils.TIKV_METRICS_ENABLE)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.metrics.enable");

    public static final ConfigOption<Boolean>  TIKV_ENABLE_GRPC_FORWARD =
            ConfigOptions.key(ConfigUtils. TIKV_ENABLE_GRPC_FORWARD)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.enable_grpc_forward");

    public static final ConfigOption<Boolean>  TIKV_ENABLE_ATOMIC_FOR_CAS =
            ConfigOptions.key(ConfigUtils. TIKV_ENABLE_ATOMIC_FOR_CAS)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.enable_atomic_for_cas");

    public static final ConfigOption<Integer> TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.default_backoff_in_ms");

    public static final ConfigOption<Integer> TIKV_RAWKV_READ_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_READ_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.read_timeout_in_ms");

    public static final ConfigOption<Integer> TIKV_RAWKV_WRITE_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_WRITE_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.write_timeout_in_ms");


    public static final ConfigOption<Integer> TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.batch_read_timeout_in_ms");


    public static final ConfigOption<Integer> TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.batch_write_timeout_in_ms");


    public static final ConfigOption<Integer> TIKV_RAWKV_SCAN_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_SCAN_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.scan_timeout_in_ms");


    public static final ConfigOption<Integer> TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.clean_timeout_in_ms");


    public static final ConfigOption<Integer> TIKV_RAWKV_SCAN_SLOWLOG_IN_MS =
            ConfigOptions.key(ConfigUtils.TIKV_RAWKV_SCAN_SLOWLOG_IN_MS)
                    .intType()
                    .noDefaultValue()
                    .withDescription("tikv.rawkv.scan_slowlog_in_ms");

    public static final ConfigOption<Boolean>  TIKV_TLS_ENABLE =
            ConfigOptions.key(ConfigUtils. TIKV_TLS_ENABLE)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.tls_enable");

    public static final ConfigOption<Boolean>  TiKV_CIRCUIT_BREAK_ENABLE =
            ConfigOptions.key(ConfigUtils. TiKV_CIRCUIT_BREAK_ENABLE)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription("tikv.circuit_break.enable");

    public static final ConfigOption<Boolean>  TIKV_WARM_UP_ENABLE =
            ConfigOptions.key(ConfigUtils. TIKV_WARM_UP_ENABLE)
                    .booleanType()
                    .noDefaultValue()
                    .withDescription(" tikv.warm_up.enable");


    public static TiConfiguration getTiConfiguration(
            final String pdAddrsStr, final String hostMapping, final Map<String, String> options) {
        final Configuration configuration = Configuration.fromMap(options);

        final TiConfiguration tiConf = TiConfiguration.createDefault(pdAddrsStr);
        Optional.of(new UriHostMapping(hostMapping)).ifPresent(tiConf::setHostMapping);
        configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
        configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
        configuration.getOptional(TIKV_BATCH_GET_CONCURRENCY).ifPresent(tiConf::setBatchGetConcurrency);
        configuration.getOptional(TIKV_BATCH_SCAN_CONCURRENCY).ifPresent(tiConf::setBatchScanConcurrency);
        configuration.getOptional(TIKV_GRPC_INGEST_TIMEOUT).ifPresent(tiConf::setIngestTimeout);
        configuration.getOptional(TIKV_GRPC_FORWARD_TIMEOUT).ifPresent(tiConf::setForwardTimeout);
        configuration.getOptional(TIKV_GRPC_WARM_UP_TIMEOUT).ifPresent(tiConf::setWarmUpTimeout);
        configuration.getOptional(TIKV_PD_FIRST_GET_MEMBER_TIMEOUT).ifPresent(tiConf::setPdFirstGetMemberTimeout);
        configuration.getOptional(TIKV_GRPC_MAX_FRAME_SIZE).ifPresent(tiConf::setMaxFrameSize);
        configuration.getOptional(TIKV_GRPC_KEEPALIVE_TIME).ifPresent(tiConf::setKeepaliveTime);
        configuration.getOptional(TIKV_GRPC_KEEPALIVE_TIMEOUT).ifPresent(tiConf::setKeepaliveTimeout);
        configuration.getOptional(TIKV_GRPC_IDLE_TIMEOUT).ifPresent(tiConf::setIdleTimeout);
        configuration.getOptional(TIKV_CONN_RECYCLE_TIME).ifPresent(tiConf::setConnRecycleTimeInSeconds);
        configuration.getOptional(TIKV_INDEX_SCAN_BATCH_SIZE).ifPresent(tiConf::setIndexScanBatchSize);
        configuration.getOptional(TIKV_TABLE_SCAN_CONCURRENCY).ifPresent(tiConf::setTableScanConcurrency);
        configuration.getOptional(TIKV_BATCH_PUT_CONCURRENCY).ifPresent(tiConf::setBatchPutConcurrency);
        configuration.getOptional(TIKV_BATCH_DELETE_CONCURRENCY).ifPresent(tiConf::setBatchDeleteConcurrency);
        configuration.getOptional(TIKV_DELETE_RANGE_CONCURRENCY).ifPresent(tiConf::setDeleteRangeConcurrency);
        configuration.getOptional(TIKV_SHOW_ROWID).ifPresent(tiConf::setShowRowId);
        configuration.getOptional(TIKV_DB_PREFIX).ifPresent(tiConf::setDBPrefix);
        configuration.getOptional(TIKV_KV_CLIENT_CONCURRENCY).ifPresent(tiConf::setKvClientConcurrency);
        configuration.getOptional(TIKV_METRICS_ENABLE).ifPresent(tiConf::setMetricsEnable);
        configuration.getOptional(TIKV_METRICS_PORT).ifPresent(tiConf::setMetricsPort);
        configuration.getOptional(TIKV_ENABLE_GRPC_FORWARD).ifPresent(tiConf::setEnableGrpcForward);
        configuration.getOptional(TIKV_GRPC_HEALTH_CHECK_TIMEOUT).ifPresent(tiConf::setGrpcHealthCheckTimeout);
        configuration.getOptional(TIKV_HEALTH_CHECK_PERIOD_DURATION).ifPresent(tiConf::setHealthCheckPeriodDuration);
        configuration.getOptional(TIKV_ENABLE_ATOMIC_FOR_CAS).ifPresent(tiConf::setEnableAtomicForCAS);
        configuration.getOptional(TIKV_IMPORTER_MAX_KV_BATCH_BYTES).ifPresent(tiConf::setImporterMaxKVBatchBytes);
        configuration.getOptional(TIKV_IMPORTER_MAX_KV_BATCH_SIZE).ifPresent(tiConf::setImporterMaxKVBatchSize);
        configuration.getOptional(TIKV_SCATTER_WAIT_SECONDS).ifPresent(tiConf::setScatterWaitSeconds);
        configuration.getOptional(TIKV_RAWKV_DEFAULT_BACKOFF_IN_MS).ifPresent(tiConf::setRawKVDefaultBackoffInMS);
        configuration.getOptional(TIKV_RAWKV_READ_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVReadTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_WRITE_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVWriteTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_BATCH_READ_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVBatchReadTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_BATCH_WRITE_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVBatchWriteTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_SCAN_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVScanTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_CLEAN_TIMEOUT_IN_MS).ifPresent(tiConf::setRawKVCleanTimeoutInMS);
        configuration.getOptional(TIKV_RAWKV_SCAN_SLOWLOG_IN_MS).ifPresent(tiConf::setRawKVScanSlowLogInMS);
        configuration.getOptional(TIKV_TLS_ENABLE).ifPresent(tiConf::setTlsEnable);
        configuration.getOptional(TIKV_TLS_RELOAD_INTERVAL).ifPresent(tiConf::setCertReloadIntervalInSeconds);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_ENABLE).ifPresent(tiConf::setCircuitBreakEnable);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_AVAILABILITY_WINDOW_IN_SECONDS).ifPresent(tiConf::setCircuitBreakAvailabilityWindowInSeconds);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_AVAILABILITY_ERROR_THRESHOLD_PERCENTAGE).ifPresent(tiConf::setCircuitBreakAvailabilityErrorThresholdPercentage);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_AVAILABILITY_REQUEST_VOLUMN_THRESHOLD).ifPresent(tiConf::setCircuitBreakAvailabilityRequestVolumnThreshold);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_SLEEP_WINDOW_IN_SECONDS).ifPresent(tiConf::setCircuitBreakSleepWindowInSeconds);
        configuration.getOptional(TiKV_CIRCUIT_BREAK_ATTEMPT_REQUEST_COUNT).ifPresent(tiConf::setCircuitBreakAttemptRequestCount);
        configuration.getOptional(TIKV_SCAN_REGIONS_LIMIT).ifPresent(tiConf::setScanRegionsLimit);
        configuration.getOptional(TIKV_WARM_UP_ENABLE).ifPresent(tiConf::setWarmUpEnable);

        return tiConf;
    }
}
