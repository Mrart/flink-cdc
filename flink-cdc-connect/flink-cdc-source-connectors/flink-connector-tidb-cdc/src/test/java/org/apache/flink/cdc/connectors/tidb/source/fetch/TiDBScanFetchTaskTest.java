package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;

import java.util.List;

/** Tests for {@link TiDBScanFetchTask}. */

/**
 * VALUES (default, "scooter", "Small 2-wheel scooter", 3.14),
 *        (default, "car battery", "12V car battery", 8.1),
 *        (default, "12-pack drill bits", "12-pack of drill bits with sizes ranging from #40 to #3", 0.8),
 *        (default, "hammer", "12oz carpenter's hammer", 0.75),
 *        (default, "hammer", "14oz carpenter's hammer", 0.875),
 *        (default, "hammer", "16oz carpenter's hammer", 1.0),
 *        (default, "rocks", "box of assorted rocks", 5.3),
 *        (default, "jacket", "water resistent black wind breaker", 0.1),
 *        (default, "spare tire", "24 inch spare tire", 22.2);
 */
public class TiDBScanFetchTaskTest extends TiDBTestBase {
    private static final String databaseName = "customer";
    private static final String tableName = "customers";
    public void testChangingDataInSnapshotScan() throws Exception {
        initializeTidbTable("customer");
        String tableId = databaseName + "." + tableName;
        String[] changingDataSql = new String[] {
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 103",
                "DELETE FROM " + tableId + " where id = 102",
                "INSERT INTO " + tableId + " VALUES(102, 'user_2','hangzhou','123567891234')",
                "UPDATE " + tableId + " SET address = 'Shanghai' where id = 103",
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 110",
                "UPDATE " + tableId + " SET address = 'Hangzhou' where id = 111",
        };
        String[] expected =
                new String[] {
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, hangzhou, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Hangzhou, 123567891234]",
                        "+I[111, user_6, Hangzhou, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                };
    }

    private List<String> getDataInSnapshotScan(
            String[] changingDataSql,
            String databaseName,
            String tableName,
            int hookType,
            boolean skipSnapshotBackfill)
            throws Exception {
        TiDBSourceConfigFactory tiDBSourceConfigFactory = new TiDBSourceConfigFactory();
        tiDBSourceConfigFactory.hostname(TIDB.getHost());
        tiDBSourceConfigFactory.port(TiDBTestBase.TIDB_PORT);
        tiDBSourceConfigFactory.username(TiDBTestBase.TIDB_USER);
        tiDBSourceConfigFactory.password(TiDBTestBase.TIDB_PASSWORD);
        tiDBSourceConfigFactory.databaseList(this.databaseName);
        tiDBSourceConfigFactory.tableList(this.tableName);
        tiDBSourceConfigFactory.splitSize(10);
        tiDBSourceConfigFactory.skipSnapshotBackfill(skipSnapshotBackfill);
        JdbcSourceConfig jdbcSourceConfig = tiDBSourceConfigFactory.create(0);
        TiDBDialect tiDBDialect = new TiDBDialect(tiDBSourceConfigFactory.create(0));
        SnapshotPhaseHooks snapshotPhaseHooks = new SnapshotPhaseHooks();

        try(TiDBConnection tiDBConnection = tiDBDialect.openJdbcConnection()) {
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
        }
        return null;
    }
}
