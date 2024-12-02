package org.apache.flink.cdc.connectors.tidb.source.connection;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.relational.connection.JdbcConnectionPoolFactory;

public class TiDBConnectionPoolFactory extends JdbcConnectionPoolFactory {
    private static final String MYSQL_URL_PATTERN =
            "jdbc:mysql://%s:%s/?useUnicode=true&useSSL=false&useInformationSchema=true&nullCatalogMeansCurrent=false&zeroDateTimeBehavior=convertToNull&characterEncoding=UTF-8&characterSetResults=UTF-8";

    @Override
    public String getJdbcUrl(JdbcSourceConfig sourceConfig) {
        String hostName = sourceConfig.getHostname();
        int port = sourceConfig.getPort();
        return String.format(MYSQL_URL_PATTERN, hostName, port);
    }
}
