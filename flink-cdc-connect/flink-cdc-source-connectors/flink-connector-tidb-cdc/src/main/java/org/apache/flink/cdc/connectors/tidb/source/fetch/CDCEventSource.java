package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.data.Envelope;
import io.debezium.function.BlockingConsumer;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.relational.TableSchema;
import io.debezium.util.Clock;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.cdc.connectors.base.relational.JdbcSourceEventDispatcher;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.apache.flink.cdc.connectors.tidb.table.utils.UriHostMapping;
import org.apache.flink.util.function.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCClientV2;
import org.tikv.cdc.ICDCClientV2;
import org.tikv.cdc.OpType;
import org.tikv.cdc.RawKVEntry;
import org.tikv.cdc.RegionFeedEvent;
import org.tikv.common.TiConfiguration;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;

import java.io.Serializable;
import java.time.Instant;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
  private final SerializableFunction<RegionFeedEvent, TableId> tableIdProvider;
  private final Map<TableSchema, Map<String, Integer>> fieldIndexMap = new HashMap<>();
  private final ICDCClientV2 cdcClientV2;

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
    this.cdcClientV2 = new CDCClientV2(getTiConfig(connectorConfig.getSourceConfig()));
  }

  private TableId getTableId(RegionFeedEvent event) {
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
        offsetContext != null ? offsetContext : CDCEventOffsetContext.initial(this.connectorConfig);
    Set<Envelope.Operation> skippedOperations = this.connectorConfig.getSkippedOperations();
    EnumMap<OpType, BlockingConsumer<RegionFeedEvent>> eventHandlers = new EnumMap<>(OpType.class);
    eventHandlers.put(OpType.Heatbeat, event -> LOG.trace("HEARTBEAT message: {}", event));
    eventHandlers.put(OpType.Ddl, event -> LOG.trace("DDL message: {}", event));
    // The tidb cdc client has already handled the transaction, so we only need to handle
    // DDL/Update/Delete/Insert;
    if (!skippedOperations.contains(Envelope.Operation.CREATE)) {
      eventHandlers.put(
          OpType.Put,
          event ->
              handleChange(partition, effectiveOffsetContext, Envelope.Operation.CREATE, event));
    }
    if (!skippedOperations.contains(Envelope.Operation.UPDATE)) {
      eventHandlers.put(
          OpType.Delete,
          event ->
              handleChange(partition, effectiveOffsetContext, Envelope.Operation.UPDATE, event));
    }
    if (!skippedOperations.contains(Envelope.Operation.DELETE)) {
      eventHandlers.put(
          OpType.Delete,
          event ->
              handleChange(partition, effectiveOffsetContext, Envelope.Operation.DELETE, event));
    }
    eventHandlers.put(OpType.Resolved, event -> LOG.trace("HEARTBEAT message: {}", event));
    this.cdcClientV2.execute(
        Long.parseLong(this.split.getStartingOffset().getOffset().get("timestamp")),
        new TableId("customer", null, "customers"));
    while (true) {
      RegionFeedEvent raw = null;
      for (int i = 0; i < 2000; i++) {
        raw = this.cdcClientV2.get();
        if (raw != null) {
          eventHandlers
              .getOrDefault(
                  raw.getRawKVEntry().getOpType(),
                  skipRaw -> LOG.trace("Skip raw message {}", skipRaw))
              .accept(raw);
          offsetContext.event(
              getTableId(raw), Instant.ofEpochSecond(this.cdcClientV2.getResolvedTs()));
        }
      }
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

  private void handleChange(
      TiDBPartition partition,
      CDCEventOffsetContext offsetContext,
      Envelope.Operation operation,
      RegionFeedEvent event)
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
    offsetContext.event(tableId, Instant.ofEpochMilli(event.getRawKVEntry().getCrts()));

    Map<String, Integer> fieldIndex =
        fieldIndexMap.computeIfAbsent(
            tableSchema,
            schema ->
                IntStream.range(0, schema.valueSchema().fields().size())
                    .boxed()
                    .collect(
                        Collectors.toMap(
                            i -> schema.valueSchema().fields().get(i).name(), i -> i)));
    Serializable[] before = null;
    Serializable[] after = null;
    final RowKey rowKey = RowKey.decode(event.getRawKVEntry().getKey().toByteArray());
    final long handle = rowKey.getHandle();
    TiTableInfo tableInfo = this.cdcClientV2.getTableInfo(event.getDbName(), event.getTableName());
    switch (event.getRawKVEntry().getOpType()) {
      case Delete:
        before = new Serializable[fieldIndex.size()];
        Object[] tikvValue =
            decodeObjects(event.getRawKVEntry().getOldValue().toByteArray(), handle, tableInfo);
        for (int i = 0; i < tikvValue.length; i++) {
          before[i] = (Serializable) tikvValue[i];
        }
      case Put:
        if (event.getRawKVEntry().isUpdate()) {
          RawKVEntry[] rawKVEntries =
              event.getRawKVEntry().splitUpdateKVEntry(event.getRawKVEntry());
          RawKVEntry deleteRawKVEntry = rawKVEntries[0];
          before = new Serializable[fieldIndex.size()];
          Object[] tiKVValueBefore =
              decodeObjects(deleteRawKVEntry.getOldValue().toByteArray(), handle, tableInfo);
          for (int i = 0; i < tiKVValueBefore.length; i++) {
            before[i] = (Serializable) tiKVValueBefore[i];
          }
          RawKVEntry insertKVEntry = rawKVEntries[1];
          after = new Serializable[fieldIndex.size()];
          Object[] tiKVValueAfter =
              decodeObjects(insertKVEntry.getValue().toByteArray(), handle, tableInfo);
          for (int i = 0; i < tiKVValueBefore.length; i++) {
            after[i] = (Serializable) tiKVValueAfter[i];
          }
        } else {
          // insert
          after = new Serializable[fieldIndex.size()];
          Object[] tiKVValueAfter =
              decodeObjects(event.getRawKVEntry().getValue().toByteArray(), handle, tableInfo);
          for (int i = 0; i < tiKVValueAfter.length; i++) {
            after[i] = (Serializable) tiKVValueAfter[i];
          }
        }
    }
    eventDispatcher.dispatchDataChangeEvent(
        partition,
        tableId,
        new CDCEventEmitter(partition, offsetContext, Clock.SYSTEM, operation, before, after));
  }

  public static TiConfiguration getTiConfig(TiDBSourceConfig tiDBSourceConfig) {
    final TiConfiguration tiConf = TiConfiguration.createDefault(tiDBSourceConfig.getPdAddresses());
    Optional.of(new UriHostMapping(tiDBSourceConfig.getHostMapping()))
        .ifPresent(tiConf::setHostMapping);
    tiConf.setGrpcHealthCheckTimeout(60000);
    tiConf.setTimeout(60000);
    // get tikv config；
    return tiConf;
  }
}
