/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.postgres.source;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.annotation.PublicEvolving;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;

import java.time.Duration;

/** Configurations for {@link PostgresDataSource}. */
@PublicEvolving
public class PostgresDataSourceOptions {

    public static final ConfigOption<String> HOSTNAME =
            ConfigOptions.key("hostname")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("IP address or hostname of the PostgreSQL database server.");
    public static final ConfigOption<Integer> PG_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(5432)
                    .withDescription("Integer port number of the PostgreSQL database server.");

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Name of the PostgreSQL database to use when connecting to the PostgreSQL database server.");

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Password to use when connecting to the PostgreSQL database server.");
    public static final ConfigOption<String> TABLES =
            ConfigOptions.key("tables")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the PostgreSQL tables to monitor. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");

    public static final ConfigOption<String> DECODING_PLUGIN_NAME =
            ConfigOptions.key("decoding.plugin.name")
                    .stringType()
                    .defaultValue("decoderbufs")
                    .withDescription(
                            "The name of the Postgres logical decoding plug-in installed on the server.\n"
                                    + "Supported values are decoderbufs, wal2json, wal2json_rds, wal2json_streaming,\n"
                                    + "wal2json_rds_streaming and pgoutput.");

    public static final ConfigOption<String> SLOT_NAME =
            ConfigOptions.key("slot.name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The name of the PostgreSQL logical decoding slot that was created for streaming changes "
                                    + "from a particular plug-in for a particular database/schema. The server uses this slot "
                                    + "to stream events to the connector that you are configuring.");

    public static final ConfigOption<DebeziumChangelogMode> CHANGELOG_MODE =
            ConfigOptions.key("changelog-mode")
                    .enumType(DebeziumChangelogMode.class)
                    .defaultValue(DebeziumChangelogMode.ALL)
                    .withDescription(
                            "The changelog mode used for encoding streaming changes.\n"
                                    + "\"all\": Encodes changes as retract stream using all RowKinds. This is the default mode.\n"
                                    + "\"upsert\": Encodes changes as upsert stream that describes idempotent updates on a key. It can be used for tables with primary keys when replica identity FULL is not an option.");

    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_ENABLED =
            ConfigOptions.key("scan.incremental.snapshot.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Incremental snapshot is a new mechanism to read snapshot of a table. "
                                    + "Compared to the old snapshot mechanism, the incremental snapshot has many advantages, including:\n"
                                    + "(1) source can be parallel during snapshot reading, \n"
                                    + "(2) source can perform checkpoints in the chunk granularity during snapshot reading, \n"
                                    + "(3) source doesn't need to acquire global read lock before snapshot reading.");

    public static final ConfigOption<String> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN =
            ConfigOptions.key("scan.incremental.snapshot.chunk.key-column")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The chunk key of table snapshot, captured tables are split into multiple chunks by a chunk key when read the snapshot of table."
                                    + "By default, the chunk key is the first column of the primary key and the chunk key is the RowId in oracle."
                                    + "This column must be a column of the primary key.");

    public static final ConfigOption<String> SERVER_TIME_ZONE =
            ConfigOptions.key("server-time-zone")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The session time zone in database server. If not set, then "
                                    + "ZoneId.systemDefault() is used to determine the server time zone.");

    public static final ConfigOption<Integer> SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE =
            ConfigOptions.key("scan.incremental.snapshot.chunk.size")
                    .intType()
                    .defaultValue(8096)
                    .withDescription(
                            "The chunk size (number of rows) of table snapshot, captured tables are split into multiple chunks when read the snapshot of table.");

    public static final ConfigOption<Integer> SCAN_SNAPSHOT_FETCH_SIZE =
            ConfigOptions.key("scan.snapshot.fetch.size")
                    .intType()
                    .defaultValue(1024)
                    .withDescription(
                            "The maximum fetch size for per poll when read table snapshot.");

    public static final ConfigOption<Duration> CONNECT_TIMEOUT =
            ConfigOptions.key("connect.timeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "The maximum time that the connector should wait after trying to connect to the PostgreSQL database server before timing out.");

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
                            "The max retry times that the connector should retry to build PostgreSQL database server connection.");

    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("initial")
                    .withDescription(
                            "Optional startup mode for PostgreSQL CDC consumer, valid enumerations are "
                                    + "\"initial\", \"earliest-offset\", \"latest-offset\", \"timestamp\"\n"
                                    + "or \"specific-offset\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_FILE =
            ConfigOptions.key("scan.startup.specific-offset.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file name used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_POS =
            ConfigOptions.key("scan.startup.specific-offset.pos")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional binlog file position used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET =
            ConfigOptions.key("scan.startup.specific-offset.gtid-set")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional GTID set used in case of \"specific-offset\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS =
            ConfigOptions.key("scan.startup.specific-offset.skip-events")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional number of events to skip after the specific starting offset");

    public static final ConfigOption<Long> SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS =
            ConfigOptions.key("scan.startup.specific-offset.skip-rows")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Optional number of rows to skip after the specific offset");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    public static final ConfigOption<Duration> HEARTBEAT_INTERVAL =
            ConfigOptions.key("heartbeat.interval")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(30))
                    .withDescription(
                            "Optional interval of sending heartbeat event for tracing the latest available binlog offsets");

    public static final org.apache.flink.configuration.ConfigOption<Double>
            SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
                    org.apache.flink.configuration.ConfigOptions.key(
                                    "chunk-key.even-distribution.factor.upper-bound")
                            .doubleType()
                            .defaultValue(1000.0d)
                            .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                            .withDescription(
                                    "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                            + " table is evenly distribution or not."
                                            + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                            + " and the query for splitting would happen when it is uneven."
                                            + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    public static final org.apache.flink.configuration.ConfigOption<Double>
            SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
                    org.apache.flink.configuration.ConfigOptions.key(
                                    "chunk-key.even-distribution.factor.lower-bound")
                            .doubleType()
                            .defaultValue(0.05d)
                            .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                            .withDescription(
                                    "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                            + " table is evenly distribution or not."
                                            + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                            + " and the query for splitting would happen when it is uneven."
                                            + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP =
            ConfigOptions.key("scan.incremental.snapshot.backfill.skip")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to skip backfill in snapshot reading phase. If backfill is skipped, changes on captured tables during snapshot phase will be consumed later in change log reading phase instead of being merged into the snapshot.WARNING: Skipping backfill might lead to data inconsistency because some change log events happened within the snapshot phase might be replayed (only at-least-once semantic is promised). For example updating an already updated value in snapshot, or deleting an already deleted entry in snapshot. These replayed change log events should be handled specially.");

    // ----------------------------------------------------------------------------
    // experimental options, won't add them to documentation
    // ----------------------------------------------------------------------------
    @Experimental
    public static final ConfigOption<Integer> CHUNK_META_GROUP_SIZE =
            ConfigOptions.key("chunk-meta.group.size")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "The group size of chunk meta, if the meta size exceeds the group size, the meta will be divided into multiple groups.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.upper-bound")
                    .doubleType()
                    .defaultValue(1000.0d)
                    .withFallbackKeys("split-key.even-distribution.factor.upper-bound")
                    .withDescription(
                            "The upper bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query PostgreSQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Double> CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND =
            ConfigOptions.key("chunk-key.even-distribution.factor.lower-bound")
                    .doubleType()
                    .defaultValue(0.05d)
                    .withFallbackKeys("split-key.even-distribution.factor.lower-bound")
                    .withDescription(
                            "The lower bound of chunk key distribution factor. The distribution factor is used to determine whether the"
                                    + " table is evenly distribution or not."
                                    + " The table chunks would use evenly calculation optimization when the data distribution is even,"
                                    + " and the query PostgreSQL for splitting would happen when it is uneven."
                                    + " The distribution factor could be calculated by (MAX(id) - MIN(id) + 1) / rowCount.");

    @Experimental
    public static final ConfigOption<Boolean> SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED =
            ConfigOptions.key("scan.incremental.close-idle-reader.enabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether to close idle readers at the end of the snapshot phase. This feature depends on "
                                    + "FLIP-147: Support Checkpoints After Tasks Finished. The flink version is required to be "
                                    + "greater than or equal to 1.14 when enabling this feature.");

    @Experimental
    public static final ConfigOption<Boolean> SCHEMA_CHANGE_ENABLED =
            ConfigOptions.key("schema-change.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether send schema change events, by default is true. If set to false, the schema changes will not be sent.");

    @Experimental
    public static final ConfigOption<String> TABLES_EXCLUDE =
            ConfigOptions.key("tables.exclude")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Table names of the PostgreSQL tables to Exclude. Regular expressions are supported. "
                                    + "It is important to note that the dot (.) is treated as a delimiter for database and table names. "
                                    + "If there is a need to use a dot (.) in a regular expression to match any character, "
                                    + "it is necessary to escape the dot with a backslash."
                                    + "eg. db0.\\.*, db1.user_table_[0-9]+, db[1-2].[app|web]_order_\\.*");
}