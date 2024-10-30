package org.apache.flink.cdc.connectors.tidb.table;

import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.JdbcUrlUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.Duration;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions.*;
import static org.apache.flink.cdc.connectors.tidb.TDBSourceOptions.PD_ADDRESSES;
import static org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions.*;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

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
        options.add(PD_ADDRESSES);
        options.add(TiDB_PORT);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(SCAN_STARTUP_MODE);
        options.add(SCAN_STARTUP_TIMESTAMP_MILLIS);

        options.add(DATABASE_NAME);
        options.add(TABLE_NAME);
        options.add(TABLE_LIST);
        options.add(CONNECT_TIMEOUT);
        options.add(SERVER_TIME_ZONE);
        options.add(HOST_MAPPING);
        options.add(JDBC_DRIVER);
//        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
//        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
//        options.add(CHUNK_META_GROUP_SIZE);
//        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
//
//        options.add(CONNECTION_POOL_SIZE);
//
//        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
//        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
//        options.add(CONNECT_MAX_RETRIES);
//        options.add(SCAN_NEWLY_ADDED_TABLE_ENABLED);
//        options.add(SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
//        options.add(HEARTBEAT_INTERVAL);
//        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
//        options.add(SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
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
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SCAN_STARTUP_MODE.key(),
                                START_MODE_VALUE_INITIAL,
                                START_MODE_VALUE_SNAPSHOT,
                                START_MODE_VALUE_LATEST_OFFSET,
                                START_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    public static final String TIKV_PROPERTIES_PREFIX = "tikv.properties.";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);

        helper.validateExcept(
                JdbcUrlUtils.PROPERTIES_PREFIX,
                TIKV_PROPERTIES_PREFIX,
                DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();

        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);
        String tableName = config.get(TABLE_NAME);
        String tableList = config.get(TABLE_LIST);

        int port = config.get(TiDB_PORT);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        String pdAddresses = config.get(PD_ADDRESSES);
        String hostMapping = config.get(HOST_MAPPING);
        String jdbcDriver = config.get(JDBC_DRIVER);

        ResolvedSchema physicalSchema =
                getPhysicalSchema(context.getCatalogTable().getResolvedSchema());

        StartupOptions startupOptions = getStartupOptions(config);

        Duration heartbeatInterval = config.get(HEARTBEAT_INTERVAL);

        OptionUtils.printOptions(IDENTIFIER, ((Configuration) config).toMap());

        return null;
    }

    private Properties getProperties(Map<String, String> tableOptions, String prefix) {
        Properties properties = new Properties();
        tableOptions.keySet().stream()
                .filter(key -> key.startsWith(prefix))
                .forEach(
                        key -> {
                            final String value = tableOptions.get(key);
                            final String subKey = key.substring(prefix.length());
                            properties.put(subKey, value);
                        });
        return properties;
    }
}
