package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CDCEventOffsetContext implements OffsetContext {
  private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

  public static final String TIMESTAMP_KEY = "timestamp";
  public static final String EVENTS_TO_SKIP_KEY = "events";

  private final Schema sourceInfoSchema;
  private final TiDBSourceInfo sourceInfo;
  private final TransactionContext transactionContext;
  private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
  private boolean snapshotCompleted;
  private long currentTimestamp;
  private long restartEventsToSkip = 0;

  public CDCEventOffsetContext(
      boolean snapshot,
      boolean snapshotCompleted,
      TransactionContext transactionContext,
      IncrementalSnapshotContext<TableId> incrementalSnapshotContext,
      TiDBSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    this.sourceInfoSchema = sourceInfo.schema();
    this.snapshotCompleted = snapshotCompleted;

    this.transactionContext = transactionContext;
    this.incrementalSnapshotContext = incrementalSnapshotContext;

    if (this.snapshotCompleted) {
      postSnapshotCompletion();
    } else {
      sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
    }
  }

  @Override
  public Map<String, ?> getOffset() {
    HashMap<String, Object> map = new HashMap<>();
    if (restartEventsToSkip != 0) {
      map.put(EVENTS_TO_SKIP_KEY, String.valueOf(restartEventsToSkip));
    }
    if (sourceInfo.timestamp() != null) {
      map.put(TIMESTAMP_KEY, String.valueOf(sourceInfo.timestamp().getEpochSecond()));
    }
    return map;
  }

  @Override
  public Schema getSourceInfoSchema() {
    return sourceInfoSchema.schema();
  }

  @Override
  public Struct getSourceInfo() {
    return sourceInfo.struct();
  }

  @Override
  public boolean isSnapshotRunning() {
    return false;
  }

  @Override
  public void markLastSnapshotRecord() {}

  @Override
  public void preSnapshotStart() {}

  @Override
  public void preSnapshotCompletion() {}

  @Override
  public void postSnapshotCompletion() {}

  @Override
  public void event(DataCollectionId collectionId, Instant timestamp) {
    sourceInfo.setSourceTime(timestamp);
    sourceInfo.tableEvent((TableId) collectionId);
    //    sourceInfo.update(instant, (TableId) tableId);
  }

  @Override
  public TransactionContext getTransactionContext() {
    return transactionContext;
  }

  public CDCEventOffsetContext(
      TiDBConnectorConfig connectorConfig,
      boolean snapshot,
      boolean snapshotCompleted,
      TiDBSourceInfo tiDBSourceInfo) {
    this(
        snapshot,
        snapshotCompleted,
        new TransactionContext(),
        connectorConfig.isReadOnlyConnection()
            ? new MySqlReadOnlyIncrementalSnapshotContext<>()
            : new SignalBasedIncrementalSnapshotContext<>(),
        tiDBSourceInfo);
  }
  //  public static CDCEventOffsetContext initial(TiDBConnectorConfig config){
  //    new CDCEventOffsetContext(config,)
  //  }

  public static CDCEventOffsetContext initial(TiDBConnectorConfig config) {
    final CDCEventOffsetContext offset =
        new CDCEventOffsetContext(config, false, false, new TiDBSourceInfo(config));
    offset.setBinlogStartPoint(); // start from the beginning of the binlog
    return offset;
  }

  public void setBinlogStartPoint() {
    this.currentTimestamp = System.currentTimeMillis();
  }

  public static class Loader implements OffsetContext.Loader<CDCEventOffsetContext> {

    private final TiDBConnectorConfig connectorConfig;

    public Loader(TiDBConnectorConfig connectorConfig) {
      this.connectorConfig = connectorConfig;
    }

    private Long readOptionalLong(Map<String, ?> offset, String key) {
      final Object obj = offset.get(key);
      return (obj == null) ? null : ((Number) obj).longValue();
    }

    @SuppressWarnings("unchecked")
    @Override
    public CDCEventOffsetContext load(Map<String, ?> offset) {
      boolean snapshot =
          Boolean.TRUE.equals(offset.get(TiDBSourceInfo.SNAPSHOT_KEY))
              || "true".equals(offset.get(TiDBSourceInfo.SNAPSHOT_KEY));
      boolean snapshotCompleted =
          Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY))
              || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
      IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
      if (connectorConfig.isReadOnlyConnection()) {
        incrementalSnapshotContext = MySqlReadOnlyIncrementalSnapshotContext.load(offset);
      } else {
        incrementalSnapshotContext = SignalBasedIncrementalSnapshotContext.load(offset);
      }
      final CDCEventOffsetContext offsetContext =
          new CDCEventOffsetContext(
              snapshot,
              snapshotCompleted,
              TransactionContext.load(offset),
              incrementalSnapshotContext,
              new TiDBSourceInfo(connectorConfig));
      return offsetContext;
    }
  }
}
