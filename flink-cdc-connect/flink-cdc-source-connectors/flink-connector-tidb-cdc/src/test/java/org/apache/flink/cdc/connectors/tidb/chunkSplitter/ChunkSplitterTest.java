package org.apache.flink.cdc.connectors.tidb.chunkSplitter;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;


public class ChunkSplitterTest extends TiDBTestBase {

    @Test
    public void testChunkSplitter() throws Exception {


        String databaseName = "customer";
        String tableName = "customers";

        initializeTidbTable("customer");

        TiDBSourceConfigFactory sourceConfigFactory = getConfigFactory(databaseName, new String[] {tableName} , 10);
        TiDBSourceConfig sourceConfig = (TiDBSourceConfig) sourceConfigFactory.create(0);
        TiDBDialect tiDBDialect = new TiDBDialect(sourceConfig);
        String tableId = databaseName + "." + tableName;
        String [] deleteDataSql =
                new String[]{
                        "DELETE FROM " + tableId + " where id = 101",
                        "DELETE FROM " + tableId + " where id = 102",
                };
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();

        try (JdbcConnection jdbcConnection = tiDBDialect.openJdbcConnection(sourceConfig)){
            SnapshotPhaseHook snapshotPhaseHook =
                (config, split)-> {
                    jdbcConnection.execute(deleteDataSql);
                    jdbcConnection.commit();
                    try {
                        Thread.sleep(500L);
                    }catch (InterruptedException e){
                        throw new RuntimeException(e);
                    }
                };
            hooks.setPreHighWatermarkAction(snapshotPhaseHook);

            //调用获取splits
            List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, tiDBDialect);

            System.out.println(snapshotSplits);
        }
    }


    //mockConfigFactory
    public static TiDBSourceConfigFactory getConfigFactory(String databaseName, String[] captureTables, int splitSize) {
        return (TiDBSourceConfigFactory)
                new TiDBSourceConfigFactory()
                        .hostname(TIDB.getHost())
                        .port(TIDB_PORT)
                        .username(TIDB_USER)
                        .password(TIDB_PASSWORD)
                        .databaseList(databaseName)
                        .tableList(captureTables)
                        .splitSize(splitSize);
    }


    private List<SnapshotSplit> getSnapshotSplits(
           TiDBSourceConfig sourceConfig, JdbcDataSourceDialect sourceDialect) {

        List<TableId> discoverTables = sourceDialect.discoverDataCollections(sourceConfig);
        final ChunkSplitter chunkSplitter = sourceDialect.createChunkSplitter(sourceConfig);

        List<SnapshotSplit> snapshotSplitList = new ArrayList<>();
        for (TableId table : discoverTables) {
            Collection<SnapshotSplit> snapshotSplits = chunkSplitter.generateSplits(table);
            snapshotSplitList.addAll(snapshotSplits);
        }
        return snapshotSplitList;
    }



}
