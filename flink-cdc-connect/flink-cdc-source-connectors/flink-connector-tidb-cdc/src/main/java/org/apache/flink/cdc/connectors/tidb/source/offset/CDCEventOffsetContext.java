package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.IncrementalSnapshotContext;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.relational.TableId;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class CDCEventOffsetContext implements OffsetContext {
  private static final String SNAPSHOT_COMPLETED_KEY = "snapshot_completed";



  private final Schema sourceInfoSchema;
  private final TiDBSourceInfo sourceInfo;
  private final TransactionContext transactionContext;
  private final IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
  private boolean snapshotCompleted;


  public CDCEventOffsetContext(boolean snapshot, boolean snapshotCompleted, TransactionContext transactionContext,
                               IncrementalSnapshotContext<TableId> incrementalSnapshotContext, TiDBSourceInfo sourceInfo) {
    this.sourceInfo = sourceInfo;
    this.sourceInfoSchema = sourceInfo.schema();
    this.snapshotCompleted = snapshotCompleted;

    this.transactionContext = transactionContext;
    this.incrementalSnapshotContext = incrementalSnapshotContext;

    if (this.snapshotCompleted) {
      postSnapshotCompletion();
    }
    else {
      sourceInfo.setSnapshot(snapshot ? SnapshotRecord.TRUE : SnapshotRecord.FALSE);
    }



  }


  @Override
  public Map<String, ?> getOffset() {
    return null;
  }

  @Override
  public Schema getSourceInfoSchema() {
    return null;
  }

  @Override
  public Struct getSourceInfo() {
    return null;
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

//    sourceInfo.update(instant, (TableId) tableId);
  }

  @Override
  public TransactionContext getTransactionContext() {
    return null;
  }

//  public CDCEventOffsetContext(TiDBConnectorConfig connectorConfig, boolean snapshot, boolean snapshotCompleted, SourceInfo sourceInfo){
//    this(snapshot, snapshotCompleted, new TransactionContext(),
//            connectorConfig.isReadOnlyConnection() ? new MySqlReadOnlyIncrementalSnapshotContext<>() : new SignalBasedIncrementalSnapshotContext<>(),
//            sourceInfo);
//  }
//  public static CDCEventOffsetContext initial(TiDBConnectorConfig config){
//    new CDCEventOffsetContext(config,)
//  }

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
      boolean snapshot = Boolean.TRUE.equals(offset.get(SourceInfo.SNAPSHOT_KEY)) || "true".equals(offset.get(SourceInfo.SNAPSHOT_KEY));
      boolean snapshotCompleted = Boolean.TRUE.equals(offset.get(SNAPSHOT_COMPLETED_KEY)) || "true".equals(offset.get(SNAPSHOT_COMPLETED_KEY));
      final String binlogFilename = (String) offset.get(SourceInfo.BINLOG_FILENAME_OFFSET_KEY);
      IncrementalSnapshotContext<TableId> incrementalSnapshotContext;
      if (connectorConfig.isReadOnlyConnection()) {
        incrementalSnapshotContext = MySqlReadOnlyIncrementalSnapshotContext.load(offset);
      }
      else {
        incrementalSnapshotContext = SignalBasedIncrementalSnapshotContext.load(offset);
      }
      final CDCEventOffsetContext offsetContext = new CDCEventOffsetContext(snapshot, snapshotCompleted,
              TransactionContext.load(offset), incrementalSnapshotContext,
              new TiDBSourceInfo(connectorConfig));
      return offsetContext;
    }
  }
}
