package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class CDCEventSource
        implements StreamingChangeEventSource<TiDBPartition, CDCEventOffsetContext> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCEventSource.class);
    private final StreamSplit split;
    private final TiDBConnectorConfig connectorConfig;
    private final JdbcSourceEventDispatcher<TiDBPartition> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final TiDBSourceFetchTaskContext taskContext;
    //  private final SerializableFunction<RawKVEntry, TableId> tableIdProvider;
    private final Map<TableSchema, Map<String, Integer>> fieldIndexMap = new HashMap<>();

    public CDCEventSource(
            TiDBConnectorConfig connectorConfig,
            JdbcSourceEventDispatcher<TiDBPartition> eventDispatcher,
            ErrorHandler errorHandler,
            TiDBSourceFetchTaskContext taskContext,
            StreamSplit split) {
        this.connectorConfig = connectorConfig;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
        this.taskContext = taskContext;
        this.split = split;
        //    this.tableIdProvider = message -> getTableId(connectorConfig, message);
        //    this.client = createClient(connectorConfig, split);
    }

    @Override
    public void execute(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            CDCEventOffsetContext offsetContext)
            throws InterruptedException {
        if (connectorConfig.getSourceConfig().getStartupOptions().isSnapshotOnly()) {
            LOG.info("Streaming is not enabled in current configuration");
            return;
        }
    }

    @Override
    public boolean executeIteration(
            ChangeEventSourceContext context,
            TiDBPartition partition,
            CDCEventOffsetContext offsetContext)
            throws InterruptedException {
        return StreamingChangeEventSource.super.executeIteration(context, partition, offsetContext);
    }

    @Override
    public void commitOffset(Map<String, ?> offset) {
        StreamingChangeEventSource.super.commitOffset(offset);
    }
}
