package org.apache.flink.cdc.connectors.tidb.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDataSource;

import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceOptions;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBSchemaUtils;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;


import static org.apache.flink.cdc.connectors.tidb.source.TiDBDataSourceOptions.*;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils.getPhysicalSchema;

/** A {@link Factory} to create {@link TiDBDataSource}. */
@Internal
public class TiDBDataSourceFactory implements DataSourceFactory {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBDataSourceFactory.class);

    public static final String IDENTIFIER = "tidb";

    @Override
    public DataSource createDataSource(Context context) {
        FactoryHelper.createFactoryHelper(this, context)
                .validateExcept(PROPERTIES_PREFIX, DEBEZIUM_OPTIONS_PREFIX);

        final Configuration config = context.getFactoryConfiguration();


        String hostname = config.get(HOSTNAME);
        String username = config.get(USERNAME);
        String password = config.get(PASSWORD);
        String databaseName = config.get(DATABASE_NAME);


        String tablesExclude = config.get(TABLES_EXCLUDE);
        String tables = config.get(TABLES);
        int port = config.get(TiDB_PORT);
        String serverTimeZone = config.get(SERVER_TIME_ZONE);
        Duration connectTimeout = config.get(CONNECT_TIMEOUT);
        String pdAddresses = config.get(PD_ADDRESSES);
        String hostMapping = config.get(HOST_MAPPING);

        //  increment snapshot options
        boolean enableParallelRead = config.get(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        int splitSize = config.get(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SCAN_SNAPSHOT_FETCH_SIZE);
        int connectionPoolSize = config.get(CONNECTION_POOL_SIZE);
        int connectMaxRetries = config.get(CONNECT_MAX_RETRIES);
        String chunkKeyColumn =
                config.getOptional(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN).orElse(null);
        double distributionFactorUpper = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower = config.get(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);

        Map<String, String> configMap = config.toMap();
        Map<String,String> tidbOption = TiKVOptions.getTiKVOptions(configMap);

        StartupOptions startupOptions = getStartupOptions(config);
        final TiConfiguration tiConf =
                TiDBSourceOptions.getTiConfiguration(pdAddresses, hostMapping, tidbOption);

        TiDBSourceConfigFactory configFactory = new TiDBSourceConfigFactory();
        configFactory.tiConfiguration(tiConf).username(username).password(password)
                .databaseList(".*")
                .tableList(".*")
                .port(port)
                .serverTimeZone(serverTimeZone)
                .connectTimeout(connectTimeout)
                .hostname(hostname)
                .connectionPoolSize(connectionPoolSize)
                .connectMaxRetries(connectMaxRetries)
                .chunkKeyColumn(chunkKeyColumn)
                .fetchSize(fetchSize)
                .distributionFactorLower(distributionFactorLower)
                .distributionFactorUpper(distributionFactorUpper)
                .startupOptions(startupOptions)
                .splitSize(splitSize)
                .splitMetaGroupSize(splitMetaGroupSize);


        List<TableId> tableIds = TiDBSchemaUtils.listTables(configFactory.create(0), databaseName);
        Selectors selectors = new Selectors.SelectorsBuilder().includeTables(tables).build();
        List<String> capturedTables = getTableList(tableIds, selectors);
        if (capturedTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Cannot find any table by the option 'tables' = " + tables);
        }
        if (tablesExclude != null) {
            Selectors selectExclude =
                    new Selectors.SelectorsBuilder().includeTables(tablesExclude).build();
            List<String> excludeTables = getTableList(tableIds, selectExclude);
            if (!excludeTables.isEmpty()) {
                capturedTables.removeAll(excludeTables);
            }
            if (capturedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        "Cannot find any table with by the option 'tables.exclude'  = "
                                + tablesExclude);
            }
        }
        configFactory.tableList(capturedTables.toArray(new String[0]));

        return new TiDBDataSource(configFactory);
    }

    @Override
    public String identifier() {
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
        options.add(TABLES);
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
        options.add(HEARTBEAT_INTERVAL);

        //      increment snapshot options
        options.add(SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(CHUNK_META_GROUP_SIZE);
        options.add(CONNECTION_POOL_SIZE);
        options.add(CONNECT_MAX_RETRIES);
        options.add(SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        return options;
    }


    private static final String START_MODE_VALUE_INITIAL = "initial";
    private static final String START_MODE_VALUE_LATEST_OFFSET = "latest-offset";
    private static final String START_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String START_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(Configuration config) {
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

    static class TiKVOptions {
        private static final String TIKV_OPTIONS_PREFIX = "tikv.";

        public static Map<String, String> getTiKVOptions(Map<String, String> properties) {
            Map<String, String> tikvOptions = new HashMap<>();

            if (hasTiKVOptions(properties)) {
                properties.keySet().stream()
                        .filter(key -> key.startsWith(TIKV_OPTIONS_PREFIX))
                        .forEach(
                                key -> {
                                    final String value = properties.get(key);
                                    tikvOptions.put(key, value);
                                });
            }
            return tikvOptions;
        }

        /**
         * Decides if the table options contains Debezium client properties that start with prefix
         * 'debezium'.
         */
        private static boolean hasTiKVOptions(Map<String, String> options) {
            return options.keySet().stream().anyMatch(k -> k.startsWith(TIKV_OPTIONS_PREFIX));
        }
    }
    private static List<ObjectPath> getChunkKeyColumnTableList(
            List<TableId> tableIds, Selectors selectors) {
        return tableIds.stream()
                .filter(selectors::isMatch)
                .map(tableId -> new ObjectPath(tableId.getSchemaName(), tableId.getTableName()))
                .collect(Collectors.toList());
    }

    private static List<String> getTableList(
            @Nullable List<TableId> tableIdList, Selectors selectors) {
        return tableIdList.stream()
                .filter(selectors::isMatch)
                .map(TableId::toString)
                .collect(Collectors.toList());
    }

}
