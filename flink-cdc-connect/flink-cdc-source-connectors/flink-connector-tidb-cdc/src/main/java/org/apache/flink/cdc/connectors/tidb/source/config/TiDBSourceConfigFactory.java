package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfigFactory;

import io.debezium.config.Configuration;

import java.util.Properties;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;
import static org.apache.flink.cdc.connectors.base.utils.EnvironmentUtils.checkSupportCheckpointsAfterTasksFinished;

/** A factory to initialize {@link TiDBSourceConfig}. */
@SuppressWarnings("UnusedReturnValue")
public class TiDBSourceConfigFactory extends JdbcSourceConfigFactory {
    private static final long serialVersionUID = 1L;
    private String compatibleMode;
    private String driverClassName = "com.mysql.cj.jdbc.Driver";
    private String pdAddresses;

    private String hostMapping;

    private Properties tikvProperties;
    private Properties jdbcProperties;

    public JdbcSourceConfigFactory compatibleMode(String compatibleMode) {
        this.compatibleMode = compatibleMode;
        return this;
    }

    public JdbcSourceConfigFactory driverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
        return this;
    }

    public JdbcSourceConfigFactory pdAddresses(String pdAddresses) {
        this.pdAddresses = pdAddresses;
        return this;
    }

    public JdbcSourceConfigFactory hostMapping(String hostMapping) {
        this.hostMapping = hostMapping;
        return this;
    }

    public JdbcSourceConfigFactory tikvProperties(Properties tikvProperties) {
        this.tikvProperties = tikvProperties;
        return this;
    }

    public JdbcSourceConfigFactory jdbcProperties(Properties jdbcProperties) {
        this.jdbcProperties = jdbcProperties;
        return this;
    }

    @Override
    public TiDBSourceConfig create(int subtask) {
        checkSupportCheckpointsAfterTasksFinished(closeIdleReaders);
        Properties props = new Properties();
        props.setProperty("database.server.name", "tidb_cdc");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("database.connect.timeout.ms", String.valueOf(connectTimeout.toMillis()));

        // table filter
        // props.put("database.include.list", String.join(",", databaseList));
        if (tableList != null) {
            props.put("table.include.list", String.join(",", tableList));
        }
        // value converter
        props.put("decimal.handling.mode", "precise");
        props.put("time.precision.mode", "adaptive_time_microseconds");
        props.put("binary.handling.mode", "bytes");

        if (jdbcProperties != null) {
            props.putAll(jdbcProperties);
        }

        if (tikvProperties != null) {
            props.putAll(tikvProperties);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        return new TiDBSourceConfig(
                compatibleMode,
                startupOptions,
                databaseList,
                tableList,
                pdAddresses,
                hostMapping,
                splitSize,
                splitMetaGroupSize,
                distributionFactorUpper,
                distributionFactorLower,
                includeSchemaChanges,
                closeIdleReaders,
                props,
                dbzConfiguration,
                driverClassName,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                chunkKeyColumn,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled);
    }
}
