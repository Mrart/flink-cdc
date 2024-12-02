package org.apache.flink.cdc.connectors.tidb.chunkSplitter;

import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkRange;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.ChunkSplitter;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.connectors.tidb.source.TiDBDialect;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.tidb.source.splitter.TiDBChunkSplitter;
import org.apache.flink.cdc.connectors.tidb.table.TiDBConnectorRegionITCase;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ChunkSplitterTest extends TiDBTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TiDBConnectorRegionITCase.class);
    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    @Test
    public void testSplitEvenlySizedChunksOverflow() {
        TiDBChunkSplitter splitter = new TiDBChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 19,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertEquals(2, res.size());
        assertEquals(ChunkRange.of(null, 2147483638), res.get(0));
        assertEquals(ChunkRange.of(2147483638, null), res.get(1));
    }

    @Test
    public void testSplitEvenlySizedChunksNormal() {
        TiDBChunkSplitter splitter = new TiDBChunkSplitter(null, null);
        List<ChunkRange> res =
                splitter.splitEvenlySizedChunks(
                        new TableId("catalog", "db", "tab"),
                        Integer.MAX_VALUE - 20,
                        Integer.MAX_VALUE,
                        20,
                        10,
                        10);
        assertEquals(3, res.size());
        assertEquals(ChunkRange.of(null, 2147483637), res.get(0));
        assertEquals(ChunkRange.of(2147483637, 2147483647), res.get(1));
        assertEquals(ChunkRange.of(2147483647, null), res.get(2));
    }

    @Test
    public void testChunkSplitter() throws Exception {

        String databaseName = "customer";
        String tableName = "customers";

        initializeTidbTable("customer");

        TiDBSourceConfigFactory sourceConfigFactory =
                getConfigFactory(databaseName, new String[] {tableName}, 10);
        TiDBSourceConfig sourceConfig = sourceConfigFactory.create(0);
        TiDBDialect tiDBDialect = new TiDBDialect(sourceConfig);
        String tableId = databaseName + "." + tableName;
        String[] deleteDataSql =
                new String[] {
                    "DELETE FROM " + tableId + " where id = 101",
                    "DELETE FROM " + tableId + " where id = 102",
                };
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();

        try (JdbcConnection jdbcConnection = tiDBDialect.openJdbcConnection(sourceConfig)) {
            SnapshotPhaseHook snapshotPhaseHook =
                    (config, split) -> {
                        jdbcConnection.execute(deleteDataSql);
                        jdbcConnection.commit();
                        try {
                            Thread.sleep(500L);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    };
            hooks.setPreHighWatermarkAction(snapshotPhaseHook);

            // 调用获取splits
            List<SnapshotSplit> snapshotSplits = getSnapshotSplits(sourceConfig, tiDBDialect);

            System.out.println(snapshotSplits);
        }
    }

    // mockConfigFactory
    public static TiDBSourceConfigFactory getConfigFactory(
            String databaseName, String[] captureTables, int splitSize) {
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
