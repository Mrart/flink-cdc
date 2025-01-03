package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.AbstractSourceInfo;
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
import java.util.Set;

import static org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset.COMMIT_VERSION_KEY;
import static org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffset.TIMESTAMP_KEY;
import static org.apache.flink.cdc.connectors.tidb.table.utils.TSOUtils.TSOToTimeStamp;
import static org.apache.flink.cdc.connectors.tidb.table.utils.TSOUtils.TimestampToTSO;

public class CDCEventOffsetContext implements OffsetContext {
  private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";

  private final Schema sourceInfoSchema;
  private final TiDBSourceInfo sourceInfo;
  private final TransactionContext transactionContext;
  private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
  private boolean snapshotCompleted;
  private String commitVersion;
  private String timestamp;

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

  public static CDCEventOffsetContext initial(TiDBConnectorConfig config) {
    return new CDCEventOffsetContext(
        false,
        false,
        new TransactionContext(),
        new SignalBasedIncrementalSnapshotContext<>(),
        new TiDBSourceInfo(config));
  }

  @Override
  public Map<String, ?> getOffset() {
    HashMap<String, Object> offset = new HashMap<>();
    if (timestamp != null) {
      offset.put(TIMESTAMP_KEY, timestamp);
    }

    if (commitVersion != null) {
      offset.put(COMMIT_VERSION_KEY, commitVersion);
    }
    if (sourceInfo.isSnapshot()) {
      if (!snapshotCompleted) {
        offset.put(AbstractSourceInfo.SNAPSHOT_KEY, true);
      }
      return offset;
    } else {
      return incrementalSnapshotContext.store(transactionContext.store(offset));
    }
  }

  public void databaseEvent(String database, Instant timestamp) {
    sourceInfo.setSourceTime(timestamp);
    sourceInfo.databaseEvent(database);
    sourceInfo.tableEvent((TableId) null);
  }

  public void tableEvent(String database, Set<TableId> tableIds, Instant timestamp) {
    sourceInfo.setSourceTime(timestamp);
    sourceInfo.databaseEvent(database);
    sourceInfo.tableEvent(tableIds);
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
    return sourceInfo.isSnapshot() && !snapshotCompleted;
  }

  @Override
  public void markLastSnapshotRecord() {
    sourceInfo.setSnapshot(SnapshotRecord.LAST);
  }

  @Override
  public void preSnapshotStart() {
    sourceInfo.setSnapshot(SnapshotRecord.TRUE);
    snapshotCompleted = false;
  }

  @Override
  public void preSnapshotCompletion() {
    snapshotCompleted = true;
  }

  @Override
  public void postSnapshotCompletion() {
    sourceInfo.setSnapshot(SnapshotRecord.FALSE);
  }

  @Override
  public void event(DataCollectionId collectionId, Instant timestamp) {
    sourceInfo.setSourceTime(timestamp);
    sourceInfo.tableEvent((TableId) collectionId);
  }

  public void event(DataCollectionId collectionId, long resolvedTs) {
    sourceInfo.setSourceTime(Instant.ofEpochMilli(TSOToTimeStamp(resolvedTs)));
    sourceInfo.tableEvent((TableId) collectionId);
    sourceInfo.setCommitVersion(resolvedTs);
  }

  @Override
  public TransactionContext getTransactionContext() {
    return transactionContext;
  }

  @Override
  public void incrementalSnapshotEvents() {
    sourceInfo.setSnapshot(SnapshotRecord.INCREMENTAL);
  }

  @Override
  public IncrementalSnapshotContext<?> getIncrementalSnapshotContext() {
    return incrementalSnapshotContext;
  }

  public void setCheckpoint(Instant timestamp, String commitVersion) {
    this.timestamp = String.valueOf(timestamp.toEpochMilli());
    if (commitVersion == null) {
      commitVersion = String.valueOf(TimestampToTSO(timestamp.toEpochMilli()));
    }
    this.commitVersion = commitVersion;
  }

  public void setCommitVersion(Instant timestamp, String commitVersion) {
    this.setCheckpoint(timestamp, commitVersion);
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

  public void setBinlogStartPoint() {
    this.sourceInfo.setSourceTime(Instant.now());
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
      String timestamp = (String) offset.get(TIMESTAMP_KEY);
      offsetContext.setCheckpoint(
          timestamp == null ? Instant.now() : Instant.ofEpochMilli(Long.parseLong(timestamp)),
          (String) offset.get(COMMIT_VERSION_KEY));
      return offsetContext;
    }
  }
}
