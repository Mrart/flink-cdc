package org.tikv.cdc;

import org.apache.flink.shaded.guava31.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;

import java.util.PriorityQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/** 区别V1，版本。1. */
public class CDCClientV2 implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCClientV2.class);

  // Output min resolvedTs
  private PriorityQueue<Long> resolvedTs = new PriorityQueue();
  private final TiSession session;
  private final Coprocessor.KeyRange keyRange;
  private final CDCConfig config;

  private Consumer<CDCEvent> eventConsumer;
  private final BlockingQueue<CDCEvent> eventsBuffer;

  public CDCClientV2(final TiSession session, final Coprocessor.KeyRange keyRange) {
    this(session, keyRange, new CDCConfig());
  }

  public CDCClientV2(
      final TiSession session, final Coprocessor.KeyRange keyRange, final CDCConfig config) {
    Preconditions.checkState(
        session.getConf().getIsolationLevel().equals(Kvrpcpb.IsolationLevel.SI),
        "Unsupported Isolation Level"); // only support SI for now
    this.session = session;
    this.keyRange = keyRange;
    this.config = config;
    eventsBuffer = new LinkedBlockingQueue<>(config.getEventBufferSize());
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
  public void close() throws Exception {}
}
