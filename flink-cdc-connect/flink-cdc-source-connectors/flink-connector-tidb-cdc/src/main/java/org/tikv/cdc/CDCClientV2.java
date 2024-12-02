package org.tikv.cdc;

import org.apache.flink.cdc.connectors.base.source.meta.split.StreamSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.io.grpc.ManagedChannel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CDCClientV2 implements ICDCClientV2 {
  private static final Logger LOGGER = LoggerFactory.getLogger(CDCClientV2.class);

  private final TiConfiguration tiConf;
  private final CDCConfig cdcConfig;
  private final TiSession tiSession;
  private final StreamSplit split;
  private final BlockingQueue<RawKVEntry> eventsBuffer;

  private Consumer<RawKVEntry> eventConsumer;

  public CDCClientV2(TiConfiguration tiConf, StreamSplit split) {
    this(tiConf, split, new CDCConfig());
  }

  public CDCClientV2(TiConfiguration tiConf, StreamSplit split, CDCConfig cdcConfig) {
    this.tiConf = tiConf;
    this.cdcConfig = cdcConfig;
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
  public void execute(final long startTs) {}

  @Override
  public long getResolvedTs() {
    return 0;
  }

  @Override
  public RawKVEntry get() {
    return eventsBuffer.poll();
  }

  @Override
  public void close() {}

  public List<RegionStateManager.SingleRegionInfo> divideToRegions(KeyRange keyRange) {
    final RangeSplitter splitter = RangeSplitter.newSplitter(tiSession.getRegionManager());

    final List<TiRegion> tiRegionList =
        splitter.splitRangeByRegion(Arrays.asList(keyRange)).stream()
            .map(RangeSplitter.RegionTask::getRegion)
            .sorted((a, b) -> Long.compare(a.getId(), b.getId()))
            .collect(Collectors.toList());
    List<RegionStateManager.SingleRegionInfo> singleRegionInfos = new ArrayList<>();
    tiRegionList.forEach(
        tiRegion -> {
          final String address =
              this.tiSession
                  .getRegionManager()
                  .getStoreById(tiRegion.getLeader().getStoreId())
                  .getStore()
                  .getAddress();
          final ManagedChannel channel =
              this.tiSession
                  .getChannelFactory()
                  .getChannel(address, tiSession.getPDClient().getHostMapping());
          // todo devide span to paritial span;
          RegionStateManager.SingleRegionInfo signalRegionInfo =
              new RegionStateManager.SingleRegionInfo(tiRegion.getVerID(), keyRange, channel);
          singleRegionInfos.add(signalRegionInfo);
        });
    return singleRegionInfos;
  }
}
