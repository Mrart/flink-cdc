package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.table.utils.UriHostMapping;
import org.apache.flink.util.function.SerializableFunction;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.kv.CDCClientV2;
import org.tikv.cdc.kv.EventListener;
import org.tikv.cdc.model.OpType;
import org.tikv.cdc.model.RawKVEntry;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;

import java.io.Serializable;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.tikv.common.ConfigUtils.TIKV_GRPC_TIMEOUT;
import static org.tikv.common.codec.TableCodec.decodeObjects;

public class CDCEventSource
        implements StreamingChangeEventSource<TiDBPartition, CDCEventOffsetContext> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCEventSource.class);
    private final StreamSplit split;
    private final TiDBConnectorConfig connectorConfig;
    private final JdbcSourceEventDispatcher<TiDBPartition> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final TiDBSourceFetchTaskContext taskContext;
    private final SerializableFunction<RawKVEntry, TableId> tableIdProvider;
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
        this.tableIdProvider = this::getTableId;
    }

    private TableId getTableId(RawKVEntry event) {
        if (StringUtils.isBlank(event.getDbName()) || StringUtils.isBlank(event.getTableName())) {
            return null;
        }
        return new TableId(event.getDbName(), null, event.getTableName());
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
        this.taskContext.getDatabaseSchema().assureNonEmptySchema();
        CDCEventOffsetContext effectiveOffsetContext =
                offsetContext != null
                        ? offsetContext
                        : CDCEventOffsetContext.initial(this.connectorConfig);
        Set<Envelope.Operation> skippedOperations = this.connectorConfig.getSkippedOperations();
        EnumMap<OpType, BlockingConsumer<RawKVEntry>> eventHandlers = new EnumMap<>(OpType.class);
        eventHandlers.put(
                OpType.Heatbeat, rawKVEntry -> LOG.trace("HEARTBEAT message: {}", rawKVEntry));
        eventHandlers.put(OpType.Ddl, rawKVEntry -> LOG.trace("DDL message: {}", rawKVEntry));
        // The tidb cdc client has already handled the transaction, so we only need to handle
        // DDL/Update/Delete/Insert;
        if (!skippedOperations.contains(Envelope.Operation.CREATE)) {
            eventHandlers.put(
                    OpType.Put,
                    (event) ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.CREATE,
                                    event));
        }
        if (!skippedOperations.contains(Envelope.Operation.UPDATE)) {
            eventHandlers.put(
                    OpType.Delete,
                    (event) ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.UPDATE,
                                    event));
        }
        if (!skippedOperations.contains(Envelope.Operation.DELETE)) {
            eventHandlers.put(
                    OpType.Delete,
                    (event) ->
                            handleChange(
                                    partition,
                                    effectiveOffsetContext,
                                    Envelope.Operation.DELETE,
                                    event));
        }
        eventHandlers.put(
                OpType.Resolved,
                (event) -> LOG.trace("HEARTBEAT message: {},resolvedTs:{}", event));

        this.split
                .getTableSchemas()
                .forEach(
                        (tableId, tableChange) -> {
                            LOG.debug("table id is {}", tableId);
                            CDCClientV2 cdcClientV2 =
                                    new CDCClientV2(
                                            getTiConfig(connectorConfig.getSourceConfig()),
                                            tableId.catalog(),
                                            tableId.table());
                            cdcClientV2.addListener(
                                    new EventListener() {
                                        @Override
                                        public void notify(RawKVEntry rawKVEntry) {
                                            if (!context.isRunning()) {
                                                cdcClientV2.close();
                                                return;
                                            }
                                            try {
                                                eventHandlers
                                                        .getOrDefault(
                                                                rawKVEntry.getOpType(), event -> {})
                                                        .accept(rawKVEntry);
                                            } catch (Exception e) {
                                                errorHandler.setProducerThrowable(e);
                                            }
                                        }

                                        @Override
                                        public void onException(ClientException e) {
                                            LOG.error("CDC event client error.", e);
                                            errorHandler.setProducerThrowable(e);
                                        }
                                    });
                            cdcClientV2.start(
                                    CDCEventOffset.getStartTs(this.split.getStartingOffset()));
                        });
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

    private void handleChange(
            TiDBPartition partition,
            CDCEventOffsetContext offsetContext,
            Envelope.Operation operation,
            RawKVEntry event)
            throws InterruptedException {
        final TableId tableId = tableIdProvider.apply(event);
        if (tableId == null) {
            LOG.warn("No valid tableId found, skipping log message: {}", event);
            return;
        }
        TableSchema tableSchema = taskContext.getDatabaseSchema().schemaFor(tableId);
        if (tableSchema == null) {
            LOG.warn("No table schema found, skipping log message: {}", event);
            return;
        }
        offsetContext.event(tableId, event.getCrts());

        Map<String, Integer> fieldIndex =
                fieldIndexMap.computeIfAbsent(
                        tableSchema,
                        schema ->
                                IntStream.range(0, schema.valueSchema().fields().size())
                                        .boxed()
                                        .collect(
                                                Collectors.toMap(
                                                        i ->
                                                                schema.valueSchema()
                                                                        .fields()
                                                                        .get(i)
                                                                        .name(),
                                                        i -> i)));
        Serializable[] before = null;
        Serializable[] after = null;
        final RowKey rowKey = RowKey.decode(event.getKey().toByteArray());
        final long handle = rowKey.getHandle();
        switch (event.getOpType()) {
            case Delete:
                before = new Serializable[fieldIndex.size()];
                Object[] tikvValue =
                        decodeObjects(
                                event.getOldValue().toByteArray(), handle, event.getTableInfo());
                for (int i = 0; i < tikvValue.length; i++) {
                    before[i] = (Serializable) tikvValue[i];
                }
            case Put:
                if (event.isUpdate()) {
                    RawKVEntry[] rawKVEntries = event.splitUpdateKVEntry(event);
                    RawKVEntry deleteRawKVEntry = rawKVEntries[0];
                    before = new Serializable[fieldIndex.size()];
                    Object[] tiKVValueBefore =
                            decodeObjects(
                                    deleteRawKVEntry.getOldValue().toByteArray(),
                                    handle,
                                    event.getTableInfo());
                    for (int i = 0; i < tiKVValueBefore.length; i++) {
                        before[i] = (Serializable) tiKVValueBefore[i];
                    }
                    RawKVEntry insertKVEntry = rawKVEntries[1];
                    after = new Serializable[fieldIndex.size()];
                    Object[] tiKVValueAfter =
                            decodeObjects(
                                    insertKVEntry.getValue().toByteArray(),
                                    handle,
                                    event.getTableInfo());
                    for (int i = 0; i < tiKVValueBefore.length; i++) {
                        after[i] = (Serializable) tiKVValueAfter[i];
                    }
                } else {
                    // insert
                    after = new Serializable[fieldIndex.size()];
                    LOG.debug(
                            "Receive value is {},key:{}.dbName:{},tableName: {},tableInfo: {}",
                            event.getValue().toByteArray(),
                            event.getKey(),
                            event.getDbName(),
                            event.getTableName(),
                            event.getTableInfo());
                    Object[] tiKVValueAfter =
                            decodeObjects(
                                    event.getValue().toByteArray(), handle, event.getTableInfo());
                    for (int i = 0; i < tiKVValueAfter.length; i++) {
                        after[i] = (Serializable) tiKVValueAfter[i];
                    }
                }
        }
        eventDispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new CDCEventEmitter(
                        partition, offsetContext, Clock.SYSTEM, operation, before, after));
    }

    public static TiConfiguration getTiConfig(TiDBSourceConfig tiDBSourceConfig) {
        final TiConfiguration tiConf =
                TiConfiguration.createDefault(tiDBSourceConfig.getPdAddresses());
        Optional.of(new UriHostMapping(tiDBSourceConfig.getHostMapping()))
                .ifPresent(tiConf::setHostMapping);
        tiConf.setTimeout(
                Long.parseLong(
                        tiDBSourceConfig
                                .getJdbcProperties()
                                .getProperty(TIKV_GRPC_TIMEOUT, "60000")));
        // get tikv config；
        return tiConf;
    }
}
