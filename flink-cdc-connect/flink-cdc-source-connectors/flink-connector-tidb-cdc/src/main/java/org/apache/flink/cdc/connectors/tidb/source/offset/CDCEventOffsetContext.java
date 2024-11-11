package org.apache.flink.cdc.connectors.tidb.source.offset;

import io.debezium.connector.mysql.MySqlReadOnlyIncrementalSnapshotContext;
import io.debezium.connector.mysql.SourceInfo;
import io.debezium.pipeline.source.snapshot.incremental.SignalBasedIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.schema.DataCollectionId;
import org.apache.flink.cdc.connectors.tidb.source.config.TiDBConnectorConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.time.Instant;
import java.util.Map;

public class CDCEventOffsetContext implements OffsetContext {
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
  public void event(DataCollectionId collectionId, Instant timestamp) {}

  @Override
  public TransactionContext getTransactionContext() {
    return null;
  }

  public CDCEventOffsetContext(TiDBConnectorConfig connectorConfig, boolean snapshot, boolean snapshotCompleted, SourceInfo sourceInfo){
    this(snapshot, snapshotCompleted, new TransactionContext(),
            connectorConfig.isReadOnlyConnection() ? new MySqlReadOnlyIncrementalSnapshotContext<>() : new SignalBasedIncrementalSnapshotContext<>(),
            sourceInfo);
  }
  public static CDCEventOffsetContext initial(TiDBConnectorConfig config){
    new CDCEventOffsetContext(config,)
  }

}
