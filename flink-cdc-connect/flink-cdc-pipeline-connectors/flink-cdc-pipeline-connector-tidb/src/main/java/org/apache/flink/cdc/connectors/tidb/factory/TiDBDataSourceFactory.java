package org.apache.flink.cdc.connectors.tidb.factory;

import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.factories.DataSourceFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.cdc.connectors.tidb.source.TiDBDataSourceOptions.*;
import static org.apache.flink.cdc.debezium.table.DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

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

        return null;
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
}
