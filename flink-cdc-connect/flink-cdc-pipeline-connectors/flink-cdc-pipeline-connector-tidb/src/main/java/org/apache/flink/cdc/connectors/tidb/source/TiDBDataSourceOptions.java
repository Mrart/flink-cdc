package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

import java.time.Duration;

/** Configurations for {@link TiDBDataSource}. */
public class TiDBDataSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the MySQL database server.");

    public static final ConfigOption<Integer> TiDB_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(4000)
                    .withDescription("Integer port number of the MySQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the database to use when connecting to the database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Password to use when connecting to the database server.");

    public static final ConfigOption<String> PD_ADDRESSES =
            ConfigOptions.key("pd-addresses")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("TiDB pd-server addresses");

    public static final ConfigOption<String> DATABASE_NAME =
            ConfigOptions.key("database-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Database name of the database to monitor.");

    public static final ConfigOption<String> TABLE_LIST =
            ConfigOptions.key("table-list")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "List of full names of tables, separated by commas, e.g. \"db1.table1, db2.table2\".");

    public static final ConfigOption<String> TABLE_NAME =
            ConfigOptions.key("table-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Table name of the database to monitor.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the database server before timing out.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .defaultValue("UTC")
                    .withDescription("The session time zone in database server.");

    public static final ConfigOption<Integer> CONNECTION_POOL_SIZE =
            ConfigOptions.key("connection.pool.size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("The connection pool size.");

    public static final ConfigOption<Integer> CONNECT_MAX_RETRIES =
            ConfigOptions.key("connect.max-retries")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            "The max retry times that the connector should retry to build database server connection.");

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

    public static final ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table."
                                    + "By default, the chunk key is the first column of the primary key and the chunk key is the RowId in oracle.");

    public static final ConfigOption<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final ConfigOption<Double> SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock (FLUSH TABLES WITH READ LOCK) before snapshot reading.\n"
                                    + "For MySQL, if you would like the source run in parallel, each parallel reader should have an unique server id, "
                                    + "so the 'server-id' must be a range like '5400-6400', and the range must be larger than the parallelism.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval.ms")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available replication slot offsets");

    public static final ConfigOption<String> TABLES_EXCLUDE =
            ConfigOptions.key("tables.exclude")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the MySQL tables to Exclude. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");

    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the MySQL tables to monitor. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");
}
