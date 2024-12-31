package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetFactory;

import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

public class TiDBStreamFetchTaskTest extends TiDBTestBase {
    private static final String databaseName = "customer";
    private static final String tableName = "customers";
    private static final String STREAM_SPLIT_ID = "stream-split";

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;
    private static final int MAX_RETRY_TIMES = 100;

    private TiDBSourceConfig sourceConfig;
    private TiDBDialect tiDBDialect;
    private CDCEventOffsetFactory cdcEventOffsetFactory;

    @Before
    public void before() {
        initializeTidbTable("customer");
        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        tiDBSourceConfigFactory.pdAddresses(
                PD.getContainerIpAddress() + ":" + PD.getMappedPort(PD_PORT_ORIGIN));
        tiDBSourceConfigFactory.hostname(TIDB.getHost());
        tiDBSourceConfigFactory.port(TIDB.getMappedPort(TIDB_PORT));
        tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
        tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
        tiDBSourceConfigFactory.databaseList(this.databaseName);
        tiDBSourceConfigFactory.tableList(this.databaseName + "." + this.tableName);
        tiDBSourceConfigFactory.splitSize(10);
        tiDBSourceConfigFactory.skipSnapshotBackfill(true);
        tiDBSourceConfigFactory.scanNewlyAddedTableEnabled(true);
        this.sourceConfig = tiDBSourceConfigFactory.create(0);
        this.tiDBDialect = new TiDBDialect(tiDBSourceConfigFactory.create(0));
        this.cdcEventOffsetFactory = new CDCEventOffsetFactory();
    }

    @Test
    public void testChangingDataInStream() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;
        String[] changingDataSql =
                new String[] {
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                    "DELETE FROM " + tableId + " where id = 102",
                    "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                    "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                    "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
                };

        List<String> actual = getDataInStreamRead(changingDataSql, USE_PRE_HIGHWATERMARK_HOOK);
    }

    private List<String> getDataInStreamRead(String[] changingDataSql, int hookType)
            throws SQLException {
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        try (TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
            SnapshotPhaseHook snapshotPhaseHook =
                    (tidbSourceConfig, split) -> {
                        tiDBConnection.execute(changingDataSql);
                        tiDBConnection.commit();
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    };
            if (hookType == USE_POST_LOWWATERMARK_HOOK) {
                hooks.setPostLowWatermarkAction(snapshotPhaseHook);
            } else if (hookType == USE_PRE_HIGHWATERMARK_HOOK) {
                hooks.setPreHighWatermarkAction(snapshotPhaseHook);
            }
        }
        return null;
    }
}
