package org.tikv.cdc;

import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;

import io.debezium.relational.TableId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;

import static org.apache.flink.cdc.connectors.tidb.source.fetch.CDCEventSource.getTiConfig;

public class CDCClientV2Test extends TiDBTestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDCClientV2Test.class);
    private static final String databaseName = "customer";
    private static final String tableName = "customers";

    @Test
    public void clientGetTest() throws SQLException {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory configFactoryOfCustomDatabase =
                getMockTiDBSourceConfigFactory(databaseName, null, tableName, 10);
        // set pd add;
        configFactoryOfCustomDatabase.pdAddresses(
                PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
        TiDBSourceConfig tiDBSourceConfig = configFactoryOfCustomDatabase.create(0);
        //    tiDBSourceConfig.
        ICDCClientV2 icdcClientV2 = new CDCClientV2(getTiConfig(tiDBSourceConfig));
        try (Connection connection = getJdbcConnection("customer");
                Statement statement = connection.createStatement()) {
            // update tidb.
            statement.execute("UPDATE customers SET address='hangzhou' WHERE id=103;");
        }
        icdcClientV2.execute(
                Instant.now().getEpochSecond(), new TableId(databaseName, null, tableName));
        while (true) {
            RegionFeedEvent regionFeedEvent = icdcClientV2.get();
            if (regionFeedEvent == null) {
                continue;
            } else {
                LOGGER.info("Receive event {}", regionFeedEvent);
                return;
            }
        }
    }
}
