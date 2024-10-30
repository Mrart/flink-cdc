package org.apache.flink.cdc.connectors.tidb.source.config;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.time.Duration;

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
}

