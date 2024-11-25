package org.tikv.cdc;

import io.debezium.connector.tidb.TiDBPartition;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.apache.flink.cdc.connectors.tidb.source.offset.CDCEventOffsetContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class CDCClientV2Impl implements CDCClientV2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCClientV2Impl.class);
  private final TiConfiguration tiConf;
  private final TiSession tiSession;
  private final StreamSplit split;

  private final BlockingQueue<RawKVEntry> eventsBuffer;

  private Consumer<RawKVEntry> eventConsumer;

  public CDCClientV2Impl(TiConfiguration tiConf, StreamSplit split) {
    this(tiConf, split, new CDCConfig());
  }

  public CDCClientV2Impl(TiConfiguration tiConf, StreamSplit split, CDCConfig cdcConfig) {
    this.tiConf = tiConf;
    this.split = split;
    this.tiSession = new TiSession(tiConf);
    eventsBuffer = new LinkedBlockingQueue<>(cdcConfig.getEventBufferSize());
    // fix: use queue.put() instead of queue.offer(), otherwise will lose event
    eventConsumer =
        (event) -> {
          // try 2 times offer.
          for (int i = 0; i < 2; i++) {
            if (eventsBuffer.offer(event)) {
              return;
            }
          }
          // else use put.
          try {
            eventsBuffer.put(event);
          } catch (InterruptedException e) {
            LOGGER.error("Events buffer put error!", e);
          }
        };
  }

  @Override
  public void execute(
      ChangeEventSource.ChangeEventSourceContext changeEventSourceContext,
      TiDBPartition tiDBPartition,
      CDCEventOffsetContext offsetContext) {}

  @Override
  public long getResolvedTs() {
    return 0;
  }

  @Override
  public RawKVEntry get() {
    return null;
  }

  @Override
  public void close() {}
}
