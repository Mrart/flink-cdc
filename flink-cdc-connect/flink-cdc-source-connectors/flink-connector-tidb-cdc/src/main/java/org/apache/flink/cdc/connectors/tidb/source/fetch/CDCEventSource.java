package org.apache.flink.cdc.connectors.tidb.source.fetch;

import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.base.source.meta.wartermark.WatermarkKind;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
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
import org.tikv.cdc.kv.CDCClient;
import org.tikv.cdc.kv.EventListener;
import org.tikv.cdc.model.OpType;
import org.tikv.cdc.model.PolymorphicEvent;
import org.tikv.cdc.model.RawKVEntry;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.tikv.common.codec.TableCodec.decodeObjects;

public class CDCEventSource
        implements StreamingChangeEventSource<TiDBPartition, CDCEventOffsetContext> {
    private static final Logger LOG = LoggerFactory.getLogger(CDCEventSource.class);
    private final StreamSplit split;
    private final TiDBConnectorConfig connectorConfig;
    private final JdbcSourceEventDispatcher<TiDBPartition> eventDispatcher;
    private final ErrorHandler errorHandler;
    private final TiDBSourceFetchTaskContext taskContext;
    private final SerializableFunction<PolymorphicEvent, TableId> tableIdProvider;
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

    private TableId getTableId(PolymorphicEvent event) {
        if (StringUtils.isBlank(event.getDatabaseName())
                || StringUtils.isBlank(event.getTableInfo().getName())) {
            return null;
        }
        return new TableId(event.getDatabaseName(), null, event.getTableInfo().getName());
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
        EnumMap<OpType, BlockingConsumer<PolymorphicEvent>> eventHandlers =
                new EnumMap<>(OpType.class);
        eventHandlers.put(OpType.Heatbeat, event -> LOG.trace("HEARTBEAT message: {}", event));
        eventHandlers.put(OpType.Ddl, event -> LOG.trace("DDL message: {}", event));
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
                (event) ->
                        LOG.trace("HEARTBEAT message: {},resolvedTs:{}", event, event.getCrTs()));
        LOG.info("Start Read change data from client.");
        List<CompletableFuture> cfList = new ArrayList<>();
        this.split
                .getTableSchemas()
                .forEach(
                        (tableId, tableChange) -> {
                            LOG.debug("Table id is {}", tableId);
                            CompletableFuture<Void> cf =
                                    CompletableFuture.runAsync(
                                            () -> {
                                                CDCClient cdcClientV2 =
                                                        new CDCClient(
                                                                connectorConfig
                                                                        .getSourceConfig()
                                                                        .getTiConfiguration(),
                                                                tableId.catalog(),
                                                                tableId.table());
                                                cdcClientV2.addListener(
                                                        new EventListener() {
                                                            @Override
                                                            public void notify(
                                                                    PolymorphicEvent event) {
                                                                if (event == null) {
                                                                    return;
                                                                }
                                                                if (!context.isRunning()) {
                                                                    cdcClientV2.close();
                                                                    return;
                                                                }
                                                                CDCEventOffset currentOffset =
                                                                        new CDCEventOffset(
                                                                                effectiveOffsetContext
                                                                                        .getOffset());
                                                                if (currentOffset.isBefore(
                                                                        split
                                                                                .getStartingOffset())) {
                                                                    return;
                                                                }
                                                                if (!CDCEventOffset
                                                                                .NO_STOPPING_OFFSET
                                                                                .equals(
                                                                                        split
                                                                                                .getEndingOffset())
                                                                        && currentOffset
                                                                                .isAtOrAfter(
                                                                                        split
                                                                                                .getEndingOffset())) {
                                                                    // send watermark event;
                                                                    try {
                                                                        eventDispatcher
                                                                                .dispatchWatermarkEvent(
                                                                                        partition
                                                                                                .getSourcePartition(),
                                                                                        split,
                                                                                        currentOffset,
                                                                                        WatermarkKind
                                                                                                .END);
                                                                    } catch (
                                                                            InterruptedException
                                                                                    e) {
                                                                        LOG.error(
                                                                                "Send signal event error.",
                                                                                e);
                                                                        errorHandler
                                                                                .setProducerThrowable(
                                                                                        new RuntimeException(
                                                                                                "Error processing log signal event",
                                                                                                e));
                                                                    }
                                                                    ((StoppableChangeEventSourceContext)
                                                                                    context)
                                                                            .stopChangeEventSource();
                                                                    cdcClientV2.close();
                                                                    return;
                                                                }

                                                                try {
                                                                    eventHandlers
                                                                            .getOrDefault(
                                                                                    event.getRawKVEntry()
                                                                                            .getOpType(),
                                                                                    pEvent -> {
                                                                                        LOG.trace(
                                                                                                "Skip cdc event {}",
                                                                                                pEvent);
                                                                                    })
                                                                            .accept(event);
                                                                } catch (Exception e) {
                                                                    LOG.error(
                                                                            "Event Handler failed!",
                                                                            e);
                                                                    errorHandler
                                                                            .setProducerThrowable(
                                                                                    e);
                                                                }
                                                            }

                                                            @Override
                                                            public void onException(
                                                                    ClientException e) {
                                                                LOG.error(
                                                                        "CDC event client error.",
                                                                        e);
                                                                errorHandler.setProducerThrowable(
                                                                        e);
                                                            }
                                                        });
                                                try {
                                                    cdcClientV2.start(
                                                            CDCEventOffset.getStartTs(
                                                                    this.split
                                                                            .getStartingOffset()));
                                                    cdcClientV2.join();
                                                } finally {
                                                    cdcClientV2.close();
                                                }
                                            });
                            cfList.add(cf);
                        });
        CompletableFuture.allOf(cfList.toArray(new CompletableFuture[0])).join();
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
            PolymorphicEvent event)
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
        offsetContext.event(tableId, event.getCrTs());

        Set<Integer> fieldIndex =
                fieldIndexConverter(event.getTableInfo().getColumns(), tableSchema);
        Serializable[] before = null;
        Serializable[] after = null;
        final RowKey rowKey = RowKey.decode(event.getRawKVEntry().getKey().toByteArray());
        final long handle = rowKey.getHandle();
        switch (event.getRawKVEntry().getOpType()) {
            case Delete:
                before =
                        (Serializable[])
                                getSerializableObject(
                                        handle,
                                        event.getRawKVEntry(),
                                        event.getTableInfo(),
                                        fieldIndex);
                break;
            case Put:
                if (event.getRawKVEntry().isUpdate()) {
                    RawKVEntry[] rawKVEntries =
                            event.getRawKVEntry().splitUpdateKVEntry(event.getRawKVEntry());
                    RawKVEntry deleteRawKVEntry = rawKVEntries[0];
                    before =
                            (Serializable[])
                                    getSerializableObject(
                                            handle,
                                            deleteRawKVEntry,
                                            event.getTableInfo(),
                                            fieldIndex);
                    RawKVEntry insertKVEntry = rawKVEntries[1];
                    after =
                            (Serializable[])
                                    getSerializableObject(
                                            handle,
                                            insertKVEntry,
                                            event.getTableInfo(),
                                            fieldIndex);
                } else {
                    // insert
                    after =
                            (Serializable[])
                                    getSerializableObject(
                                            handle,
                                            event.getRawKVEntry(),
                                            event.getTableInfo(),
                                            fieldIndex);
                }
                break;
        }
        if (before == null && after == null){
            return;
        }
        eventDispatcher.dispatchDataChangeEvent(
                partition,
                tableId,
                new CDCEventEmitter(
                        partition, offsetContext, Clock.SYSTEM, operation, before, after));
    }

    private Object[] getSerializableObject(
            long handle, RawKVEntry rawKVEntry, TiTableInfo tableInfo, Set<Integer> fieldIndex) {
        Object[] serializableObject = new Serializable[fieldIndex.size()];
        try {
            if (
                rawKVEntry == null || rawKVEntry.getValue() == null){
                return null;
            }

            Object[] tiKVValueAfter =
                    decodeObjects(rawKVEntry.getValue().toByteArray(), handle, tableInfo);
            for (int index : fieldIndex) {
                serializableObject[index] = tiKVValueAfter[index];
            }
        }catch (Exception e){
            LOG.error("decode object error",e);
        }
        return serializableObject;
    }

    private Set<Integer> fieldIndexConverter(
            List<TiColumnInfo> tiColumnInfos, TableSchema tableSchema) {
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
        Set<Integer> fieldIndexSet = new HashSet<>();
        for (TiColumnInfo tiColumnInfo : tiColumnInfos) {
            if (fieldIndex.containsKey(tiColumnInfo.getName())) {
                fieldIndexSet.add(tiColumnInfo.getOffset());
            }
        }
        return fieldIndexSet;
    }
}
