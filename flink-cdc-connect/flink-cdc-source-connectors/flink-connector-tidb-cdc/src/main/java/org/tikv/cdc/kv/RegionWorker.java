package org.tikv.cdc.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.model.EventFeedStream;
import org.tikv.cdc.model.RegionFeedEvent;
import org.tikv.cdc.model.RegionKeyRange;
import org.tikv.cdc.model.RegionStatefulEvent;
import org.tikv.common.TiSession;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

/**
 * A Region worker is responsible for all regions in a TiKV store. The Region worker reads the grpc
 * response from its input chan, processes it, and writes it to Puller's eventChan.
 */
public class RegionWorker {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegionWorker.class);
  private final ExecutorService executorService;
  private final TiSession tiSession;
  private final EventFeedStream stream;
  private final Consumer<RegionFeedEvent> eventConsumer;
  private final CDCConfig cdcConfig;
  private final RegionStateManager.RegionStateManagerImpl rstManager;
  private final int workerConcurrency;
  private final LinkedBlockingQueue<RtsUpdateEvent> rtsUpdateQueque = new LinkedBlockingQueue();

  public RegionWorker(
      TiSession tiSession,
      EventFeedStream stream,
      Consumer<RegionFeedEvent> eventConsumer,
      CDCConfig cdcConfig) {
    this.tiSession = tiSession;
    this.stream = stream;
    this.eventConsumer = eventConsumer;
    this.cdcConfig = cdcConfig;
    this.workerConcurrency = cdcConfig.getWorkerPoolSize();
    this.executorService = Executors.newFixedThreadPool(cdcConfig.getWorkerPoolSize());
    this.rstManager = new RegionStateManager.RegionStateManagerImpl(cdcConfig.getWorkerPoolSize());
  }

  public RegionStateManager.RegionFeedState getRegionState(long regionId) {
    return rstManager.getState(regionId);
  }

  public void setRegionState(long regionId, RegionStateManager.RegionFeedState state) {
    rstManager.setState(regionId, state);
  }

  public int inputCalcSlot(long regionId) {
    return (int) (regionId % workerConcurrency);
  }

  public EventFeedStream getStream() {
    return stream;
  }

  public void processEvent(RegionStatefulEvent event) {
    boolean skipEvent = event.getRegionFeedState() != null && event.getRegionFeedState().isStale();
    if (skipEvent) {
      return;
    }
    if (event.getEvent() != null) {
      if (event.getEvent().hasEntries()) {
        handleEventEntry(event.getEvent().getEntries(), event.getRegionFeedState());
      }
      if (event.getEvent().hasAdmin()) {
        LOGGER.info("receive admin event.requestId:{}", event.getEvent().getRequestId());
      }
      if (event.getEvent().hasError()) {
        // 错误处理
      }
      if (event.getEvent().hasResolvedTs()) {
        RegionStatefulEvent.ResolvedTsEvent resolvedTsEvent =
            new RegionStatefulEvent.ResolvedTsEvent();
        resolvedTsEvent.setResolvedTs(event.getEvent().getResolvedTs());
        resolvedTsEvent.setRegions(Collections.singletonList(event.getRegionFeedState()));
        handleResolvedTs(resolvedTsEvent);
      }
    }
  }

  public void handleEventEntry(
      Cdcpb.Event.Entries entries, RegionStateManager.RegionFeedState state) {
    Coprocessor.KeyRange keyRange = state.getKeyRange();
    long regionId = state.getRegionId();
    for (Cdcpb.Event.Row event : entries.getEntriesList()) {
      ByteString key = event.getKey();
      switch (event.getType()) {
        case INITIALIZED:
          state.isInitialized();
          for (Cdcpb.Event.Row row : state.getMatcher().matchCachedRow(true)) {
            RegionFeedEvent regionFeedEvent = RegionFeedEvent.assembleRowEvent(regionId, row);
            eventConsumer.accept(regionFeedEvent);
          }
          state.getMatcher().matchCachedRollbackRow(true);
        case COMMITTED:
          long resolveTs = state.getLastResolvedTs();
          if (event.getCommitTs() <= resolveTs) {
            LOGGER.error(
                "The CommitTs must be greater than the resolvedTs.EventTyp:{},CommitTs:{},resolvedTs:{},regionId:{}",
                "COMMITTED",
                event.getCommitTs(),
                resolveTs,
                regionId);
            // todo
            RegionFeedEvent regionFeedEvent = RegionFeedEvent.assembleRowEvent(regionId, event);
            eventConsumer.accept(regionFeedEvent);
          }

        case PREWRITE:
          state.getMatcher().putPrewriteRow(event);
        case COMMIT:
          if (!state.getMatcher().matchRow(event, state.isInitialized())) {
            if (!state.isInitialized()) {
              state.getMatcher().cacheCommitRow(event);
              continue;
            }
            // todo ErrPrewriteNotMatch
            return;
          }
          // boolean isStaleEvent = event.getCommitTs() < startTs;
          long resolvedTs = state.getLastResolvedTs();
          if (event.getCommitTs() < resolvedTs) {
            LOGGER.error(
                "The CommitTs must be greater than the resolvedTs.EventTyp:{},CommitTs:{},resolvedTs:{},regionId:{}",
                "COMMIT",
                event.getCommitTs(),
                resolvedTs,
                regionId);
            return;
            // todo errUnreachable
          }
          RegionFeedEvent regionFeedEvent = RegionFeedEvent.assembleRowEvent(regionId, event);
          eventConsumer.accept(regionFeedEvent);
        case ROLLBACK:
          if (!state.isInitialized()) {
            state.getMatcher().cacheRollbackRow(event);
            continue;
          }
          state.getMatcher().rollbackRow(event);
        default:
          LOGGER.warn(
              "Unhandler event entry.eventType:{},eventKey:{}, regionId:{}",
              event.getType(),
              event.getKey(),
              regionId);
      }
    }
  }

  public void handleResolvedTs(RegionStatefulEvent.ResolvedTsEvent resolvedTsEvent) {
    long resovledTs = resolvedTsEvent.getResolvedTs();
    List<RegionKeyRange> regionKeyRange = new ArrayList<>();
    List<Long> regions = new ArrayList<>();
    for (RegionStateManager.RegionFeedState state : resolvedTsEvent.getRegions()) {
      if (state.isStale() || !state.isInitialized()) {
        continue;
      }
      long regionID = state.getRegionId();
      regions.add(regionID);
      long lastResolvedTs = state.getLastResolvedTs();
      if (resovledTs < lastResolvedTs) {
        LOGGER.debug(
            "The resolvedTs is fallen back in kvclient.EventType:{},resolvedTs:{},lastResolvedTs{},regionId:{}",
            "RESOLVED",
            resovledTs,
            lastResolvedTs,
            regionID);
        continue;
      }
      regionKeyRange.add(new RegionKeyRange(regionID, state.getSri().getSpan()));
    }
    if (regionKeyRange.size() == 0) {
      return;
    }
    rtsUpdateQueque.offer(new RtsUpdateEvent(regions, resovledTs));
    for (RegionStateManager.RegionFeedState state : resolvedTsEvent.getRegions()) {
      if (state.isStale() || !state.isInitialized()) {
        continue;
      }
      state.updateResolvedTs(resovledTs);
    }
    RegionFeedEvent rEvent = new RegionFeedEvent();
    RegionFeedEvent.ResolvedKeyRanges resolvedKeyRanges = new RegionFeedEvent.ResolvedKeyRanges();
    resolvedKeyRanges.setResolvedTs(resovledTs);
    resolvedKeyRanges.setKeyRanges(regionKeyRange);
    rEvent.setResolved(resolvedKeyRanges);
    // emit resolvedTs
    eventConsumer.accept(rEvent);
  }

  public void processEvents(List<RegionStatefulEvent> rsEvents) {
    for (RegionStatefulEvent rse : rsEvents) {
      processEvent(rse);
    }
  }

  public int getWorkerConcurrency() {
    return workerConcurrency;
  }

  public static class RtsUpdateEvent {
    List<Long> regions;
    long resolvedTs;

    public RtsUpdateEvent(List<Long> regions, long resolvedTs) {
      this.regions = regions;
      this.resolvedTs = resolvedTs;
    }

    public List<Long> getRegions() {
      return regions;
    }

    public void setRegions(List<Long> regions) {
      this.regions = regions;
    }

    public long getResolvedTs() {
      return resolvedTs;
    }

    public void setResolvedTs(long resolvedTs) {
      this.resolvedTs = resolvedTs;
    }
  }
}
