package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CDCEventSource
    implements StreamingChangeEventSource<TiDBPartition, CDCEventOffsetContext> {
  private static final Logger LOG = LoggerFactory.getLogger(CDCEventSource.class);

  @Override
  public void execute(
      ChangeEventSourceContext context,
      TiDBPartition partition,
      CDCEventOffsetContext offsetContext)
      throws InterruptedException {}

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
