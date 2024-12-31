package org.tikv.cdc;

import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.kv.CDCClientV2;
import org.tikv.cdc.kv.EventListener;
import org.tikv.cdc.model.PolymorphicEvent;
import org.tikv.common.meta.TiTableInfo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.Optional;


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
        CDCClientV2 icdcClientV2 =
                new CDCClientV2(tiDBSourceConfig.getTiConfiguration(), databaseName, tableName);
        try (Connection connection = getJdbcConnection(databaseName);
                Statement statement = connection.createStatement()) {
            // update tidb.
            statement.execute("UPDATE customers SET address='hangzhou' WHERE id=103;");
        }
        Optional<TiTableInfo> tableInfoOptional =
                icdcClientV2.getTableInfo(databaseName, tableName);
        icdcClientV2.addListener(
                new EventListener() {
                    @Override
                    public void notify(PolymorphicEvent rawKVEntry) {
                        Assert.assertNotNull(rawKVEntry);
                    }

                    @Override
                    public void onException(ClientException e) {}
                });
        icdcClientV2.start(Instant.now().toEpochMilli());
        icdcClientV2.join();
    }
}
