package org.tikv.cdc;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;

public interface CDCClientV2 {
  void execute(
      ChangeEventSource.ChangeEventSourceContext changeEventSourceContext,
      TiDBPartition tiDBPartition,
      CDCEventOffsetContext offsetContext);

  long getResolvedTs();

  /** @return null if no more data */
  RawKVEntry get();

  void close();
}
