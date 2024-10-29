package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;
import java.time.Duration;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.*;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.HEARTBEAT_INTERVAL;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.TiDB_PORT;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;
import static org.apache.flink.util.Preconditions.checkState;

public class NewTiDBTableSourceFactory implements DynamicTableSourceFactory {
    private static final String IDENTIFIER = "tidb-cdc";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOSTNAME);
        options.add(USERNAME);
        options.add(PASSWORD);
        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(TiDB_PORT);
        options.add(SERVER_TIME_ZONE);
        options.add(SERVER_ID);
        options.add(SCAN_STARTUP_MODE);
        //no specific offset mode
//        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
//        options.add(SCAN_STARTUP_SPECIFIC_OFFSET_POS);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(CONNECT_TIMEOUT);
        options.add(CONNECTION_POOL_SIZE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(CONNECT_MAX_RETRIES);
        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(HEARTBEAT_INTERVAL);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        return options;
    }

    private static final String START_MODE_VALUE_INITIAL = "initial";
    private static final String START_MODE_VALUE_LATEST_OFFSET = "latest-offset";
    private static final String START_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String START_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SCAN_STARTUP_MODE);
        Long startupTimestamp = config.get(SCAN_STARTUP_TIMESTAMP_MILLIS);
        switch (modeString.toLowerCase()) {
            case START_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case START_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case START_MODE_VALUE_LATEST_OFFSET:
                return StartupOptions.latest();
            case START_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(startupTimestamp);
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                START_MODE_VALUE_INITIAL,
                                START_MODE_VALUE_SNAPSHOT,
                                START_MODE_VALUE_LATEST_OFFSET,
                                modeString));
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
//        helper.validateExcept(
//                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX, JdbcUrlUtils.PROPERTIES_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        int port = config.get(TiDB_PORT);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        StartupOptions startupOptions = getStartupOptions(config);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        int connectPoolSize = config.get(CONNECTION_POOL_SIZE);
        double distributionFactorUpper =
                config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);


        boolean scanNewlyAddedTableEnabled =
                config.get(SCAN_NEWLY_ADDED_TABLE_ENABLED);
        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);
        String chunkKeyColumn =
                config.getOptional(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        boolean closeIdleReaders =
                config.get(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackFill =
                config.get(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);

        if (enableParallelRead){
            validatePrimaryKeyIfEnableParallel(physicalSchema, chunkKeyColumn);
            validateIntegerOption(
                    SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
            validateIntegerOption(CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
            validateIntegerOption(SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
            validateIntegerOption(CONNECTION_POOL_SIZE, connectPoolSize, 1);
            validateIntegerOption(CONNECT_MAX_RETRIES, connectMaxRetries, 0);
            validateDistributionFactorUpper(distributionFactorUpper);
            validateDistributionFactorLower(distributionFactorLower);
            validateDurationOption(
                   CONNECT_TIMEOUT, connectTimeout, Duration.ofMillis(250));
        }
        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return null;
    }

    private void validatePrimaryKeyIfEnableParallel(
            ResolvedSchema physicalSchema, @Nullable String chunkKeyColumn) {
        if (chunkKeyColumn == null && !physicalSchema.getPrimaryKey().isPresent()) {
            throw new ValidationException(
                    String.format(
                            "'%s' is required for table without primary key when '%s' enabled.",
                            SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN.key(),
                            SCAN_INCREMENTAL_SNAPSHOT_ENABLED.key()));
        }
    }

    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    private void validateDurationOption(
            ConfigOption<Duration> option, Duration optionValue, Duration exclusiveMin) {
        checkState(
                optionValue.toMillis() > exclusiveMin.toMillis(),
                String.format(
                        "The value of option '%s' cannot be less than %s, but actual is %s",
                        option.key(), exclusiveMin, optionValue));
    }

    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        checkState(
                ObjectUtils.doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    private void validateDistributionFactorLower(double distributionFactorLower) {
        checkState(
                ObjectUtils.doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && ObjectUtils.doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }

}
