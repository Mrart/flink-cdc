package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.data.Envelope;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalChangeRecordEmitter;
import io.debezium.util.Clock;

public class CDCEventEmitter extends RelationalChangeRecordEmitter<TiDBPartition> {
  public CDCEventEmitter(TiDBPartition partition, OffsetContext offsetContext, Clock clock) {
    super(partition, offsetContext, clock);
  }

  @Override
  protected Object[] getOldColumnValues() {
    return new Object[0];
  }

  @Override
  protected Object[] getNewColumnValues() {
    return new Object[0];
  }

  @Override
  public Envelope.Operation getOperation() {
    return null;
  }
}
