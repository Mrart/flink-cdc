package org.apache.flink.cdc.connectors.tidb.source.fetch;

import io.debezium.pipeline.source.spi.ChangeEventSource;

public class StoppableChangeEventSourceContext
    implements ChangeEventSource.ChangeEventSourceContext {

  private volatile boolean isRunning = true;

  public void stopChangeEventSource() {
    isRunning = false;
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }
}
