package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.base.source.reader.external.AbstractScanFetchTask;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.connection.TiDBConnection;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.source.schema.TiDBDatabaseSchema;
import org.apache.flink.cdc.connectors.tidb.utils.TiDBUtils;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;

public class TiDBScanFetchTask extends AbstractScanFetchTask {
    public TiDBScanFetchTask(SnapshotSplit split) {
        super(split);
    }

    private static final Logger LOG = LoggerFactory.getLogger(TiDBScanFetchTask.class);

    @Override
    protected void executeBackfillTask(Context context, StreamSplit backfillStreamSplit)
            throws Exception {

        // just for test
        TiDBSourceFetchTaskContext ctx = (TiDBSourceFetchTaskContext) context;
        final CDCEventOffset currentOffset =
                CDCEventOffset.of(
                        ((TiDBSourceFetchTaskContext) context).getOffsetContext().getOffset());
        JdbcSourceEventDispatcher dispatcher = ctx.getDispatcher();
        dispatcher.dispatchWatermarkEvent(
                ctx.getPartition().getSourcePartition(),
                backfillStreamSplit,
                currentOffset,
                WatermarkKind.END);
    }

    /** 创建并执行一个 TiDBSnapshotSplitReadTask */
    @Override
    protected void executeDataSnapshot(Context context) throws Exception {
        TiDBSourceFetchTaskContext sourceFetchContext = (TiDBSourceFetchTaskContext) context;
        TiDBSnapshotSplitReadTask tiDBSnapshotSplitReadTask =
                new TiDBSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getDispatcher(),
                        snapshotSplit);
        TiDBSnapshotSplitChangeEventSourceContext tiDBSnapshotSplitChangeEventSourceContext =
                new TiDBSnapshotSplitChangeEventSourceContext();
        SnapshotResult<CDCEventOffsetContext> snapshotResult =
                tiDBSnapshotSplitReadTask.execute(
                        tiDBSnapshotSplitChangeEventSourceContext,
                        sourceFetchContext.getPartition(),
                        sourceFetchContext.getOffsetContext());

        if (!snapshotResult.isCompletedOrSkipped()) {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for tidb split %s fail", snapshotResult));
        }
    }

    /** A wrapped task to fetch snapshot split of table. 负责从TiDB读取快照分片 */
    public static class TiDBSnapshotSplitReadTask
            extends AbstractSnapshotChangeEventSource<TiDBPartition, CDCEventOffsetContext> {

        private static final Logger LOG = LoggerFactory.getLogger(TiDBSnapshotSplitReadTask.class);
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);
        private final TiDBConnectorConfig connectorConfig;
        private final TiDBDatabaseSchema databaseSchema;

        private final TiDBConnection jdbcConnection;

        private final JdbcSourceEventDispatcher<TiDBPartition> dispatcher;
        private final Clock clock;

        private final SnapshotSplit snapshotSplit;

        private final CDCEventOffsetContext offsetContext;
        private final SnapshotProgressListener<TiDBPartition> snapshotProgressListener;

        public TiDBSnapshotSplitReadTask(
                TiDBConnectorConfig connectorConfig,
                CDCEventOffsetContext previousOffset,
                SnapshotProgressListener<TiDBPartition> snapshotProgressListener,
                TiDBConnection jdbcConnection,
                TiDBDatabaseSchema databaseSchema,
                JdbcSourceEventDispatcher<TiDBPartition> dispatcher,
                SnapshotSplit snapshotSplit) {
            super(connectorConfig, snapshotProgressListener);
            this.connectorConfig = connectorConfig;
            this.databaseSchema = databaseSchema;
            this.jdbcConnection = jdbcConnection;
            this.dispatcher = dispatcher;
            this.snapshotSplit = snapshotSplit;
            this.offsetContext = previousOffset;
            this.snapshotProgressListener = snapshotProgressListener;
            this.clock = Clock.SYSTEM;
        }

        @Override
        public SnapshotResult<CDCEventOffsetContext> execute(
                ChangeEventSourceContext context,
                TiDBPartition partition,
                CDCEventOffsetContext previousOffset)
                throws InterruptedException {
            // todo 返回为null
            SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
            final TiDBSnapshotContext ctx;
            try {
                ctx = prepare(partition);
            } catch (Exception e) {
                LOG.error("Failed to initialize snapshot context.", e);
                throw new RuntimeException(e);
            }
            try {
                return doExecute(context, previousOffset, ctx, snapshottingTask);
            } catch (InterruptedException e) {
                LOG.warn("Snapshot was interrupted before completion");
                throw e;
            } catch (Exception e) {
                LOG.warn("Snapshot was interrupted before completion");
                throw new RuntimeException(e);
            }
        }

        private static class TiDBSnapshotContext
                extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                TiDBPartition, CDCEventOffsetContext> {

            public TiDBSnapshotContext(TiDBPartition partition) throws SQLException {
                super(partition, "");
            }
        }

        @Override
        protected SnapshotResult<CDCEventOffsetContext> doExecute(
                ChangeEventSourceContext context,
                CDCEventOffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask) // 没有调用这个参数
                throws Exception {
            final TiDBSnapshotContext ctx = (TiDBSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;
            createDataEvents(ctx, snapshotSplit.getTableId());

            return SnapshotResult.completed(ctx.offset);
        }

        private void createDataEvents(TiDBSnapshotContext snapshotContext, TableId tableId)
                throws Exception {
            EventDispatcher.SnapshotReceiver<TiDBPartition> snapshotReceiver =
                    dispatcher.getSnapshotChangeEventReceiver();
            LOG.debug("Snapshotting table {}", tableId);
            createDataEventsForTable(
                    snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
            snapshotReceiver.completeSnapshot();
        }

        private void createDataEventsForTable(
                TiDBSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver<TiDBPartition> snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());



            final String selectSql =
                    TiDBUtils.buildSplitScanQuery(
                            snapshotSplit.getTableId(),
                            snapshotSplit.getSplitKeyType(),
                            snapshotSplit.getSplitStart() == null,
                            snapshotSplit.getSplitEnd() == null);
            LOG.info(
                    "For split '{}' of table {} using select statement: '{}'",
                    snapshotSplit.splitId(),
                    table.id(),
                    selectSql);

            try (PreparedStatement selectStatement =
                         TiDBUtils.readTableSplitDataStatement(
                                 jdbcConnection,
                                 selectSql,
                                 snapshotSplit.getSplitStart() == null,
                                 snapshotSplit.getSplitEnd() == null,
                                 snapshotSplit.getSplitStart(),
                                 snapshotSplit.getSplitEnd(),
                                 snapshotSplit.getSplitKeyType().getFieldCount(),
                                 connectorConfig.getQueryFetchSize());
                 ResultSet rs = selectStatement.executeQuery()) {
                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row =
                            jdbcConnection.rowToArray(table, databaseSchema, rs, columnArray);
                    if (logTimer.expired()) {
                        long stop = clock.currentTimeInMillis();
                        LOG.info(
                                "Exported {} records for split '{}' after {}",
                                rows,
                                snapshotSplit.splitId(),
                                Strings.duration(stop - exportStart));
                        snapshotProgressListener.rowsScanned(
                                snapshotContext.partition, table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    dispatcher.dispatchSnapshotEvent(
                            snapshotContext.partition,
                            table.id(),
                            getChangeRecordEmitter(snapshotContext, table.id(), row),
                            snapshotReceiver);
                }
                LOG.info(
                        "Finished exporting {} records for split '{}', total duration '{}'",
                        rows,
                        snapshotSplit.splitId(),
                        Strings.duration(clock.currentTimeInMillis() - exportStart));
            } catch (SQLException e) {
                throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
            }
        }

        protected ChangeRecordEmitter<TiDBPartition> getChangeRecordEmitter(
                TiDBSnapshotContext snapshotContext, TableId tableId, Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter<>(
                    snapshotContext.partition, snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        @Override
        protected SnapshottingTask getSnapshottingTask(
                TiDBPartition partition, CDCEventOffsetContext previousOffset) {
            return new SnapshottingTask(false, true);
        }

        @Override
        protected TiDBSnapshotContext prepare(TiDBPartition partition) throws Exception {
            return new TiDBSnapshotContext(partition);
        }
    }

    public class TiDBSnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
