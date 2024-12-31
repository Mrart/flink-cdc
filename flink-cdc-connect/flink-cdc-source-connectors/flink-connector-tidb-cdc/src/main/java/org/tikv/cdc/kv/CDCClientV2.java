package org.tikv.cdc.kv;

import org.apache.flink.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.IDAllocator;
import org.tikv.cdc.TableStoreStat;
import org.tikv.cdc.TableStoreStats;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.model.EventFeedStream;
import org.tikv.cdc.model.PolymorphicEvent;
import org.tikv.cdc.model.RawKVEntry;
import org.tikv.cdc.model.RegionFeedEvent;
import org.tikv.cdc.model.RegionStatefulEvent;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class CDCClientV2 {
    private static final Logger LOGGER = LoggerFactory.getLogger(CDCClientV2.class);
    private final CDCConfig cdcConfig;
    private final TiSession tiSession;
    private final BlockingQueue<RegionFeedEvent> eventsBuffer;

    private final ConcurrentHashMap<String, EventFeedStream> storeStreamCache =
            new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<RegionStatefulEvent> resolveTsPool =
            new ConcurrentLinkedQueue<>();
    private final TableStoreStats tableStoreStats = new TableStoreStats();
    private final AtomicLong resolvedTs = new AtomicLong(1735089007000L << 18);
    private final Consumer<RegionFeedEvent> eventConsumer;
    private final List<EventListener> listeners = new ArrayList<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ExecutorService clientExecutor = Executors.newFixedThreadPool(1);
    private final List<CompletableFuture> completableFutures = new ArrayList<>(1);
    private final String dbName;
    private final String tableName;

    public CDCClientV2(TiConfiguration tiConf, String dbName, String tableName) {
        this(tiConf, new CDCConfig(), dbName, tableName);
    }

    public CDCClientV2(
            TiConfiguration tiConf, CDCConfig cdcConfig, String dbName, String tableName) {
        this.cdcConfig = cdcConfig;
        this.tiSession = new TiSession(tiConf);
        this.dbName = dbName;
        this.tableName = tableName;
        resolveTsPool.add(
                new RegionStatefulEvent.Builder()
                        .setResolvedTsEvent(new RegionStatefulEvent.ResolvedTsEvent())
                        .build());
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

    public void start(final long startTs) {
        LOGGER.debug(
                "Start cdc client at time {},database {} table {} listening.",
                startTs,
                dbName,
                tableName);
        if (started.compareAndSet(false, true)) {
            resolvedTs.getAndSet(startTs);
            CompletableFuture<Void> completableFuture =
                    CompletableFuture.runAsync(
                            () -> { // try insert retry Mechanism
                                Optional<TiTableInfo> tableInfoOptional =
                                        getTableInfo(dbName, tableName);
                                if (!tableInfoOptional.isPresent()) {
                                    LOGGER.error(
                                            "Get tableInfo for {}.{} failed!", dbName, tableName);
                                    throw new ClientException(
                                            String.format(
                                                    "Get tableInfo for %s.%s failed.",
                                                    dbName, tableName));
                                }
                                KeyRange keyRange =
                                        TableKeyRangeUtils.getTableKeyRange(
                                                tableInfoOptional.get().getId());
                                List<RegionStateManager.SingleRegionInfo> singleRegionInfos =
                                        divideToRegions(keyRange);
                                for (RegionStateManager.SingleRegionInfo sri : singleRegionInfos) {
                                    requestRegionToStore(
                                            sri, startTs, tableInfoOptional.get().getId());
                                }
                                while (isRunning()) {
                                    for (EventListener eventListener : listeners) {
                                        RegionFeedEvent regionFeedEvent = eventsBuffer.poll();
                                        if (regionFeedEvent == null) {
                                            continue;
                                        }
                                        regionFeedEvent.setResolvedTs(this.getResolvedTs());
                                        if (regionFeedEvent.getResolved() != null) {
                                            resolvedTs.getAndSet(
                                                    regionFeedEvent.getResolved().getResolvedTs());
                                        }
                                        RawKVEntry rawKVEntry = regionFeedEvent.getRawKVEntry();
                                        PolymorphicEvent polymorphicEvent =
                                                new PolymorphicEvent(
                                                        rawKVEntry.getStartTs(),
                                                        rawKVEntry.getCrts(),
                                                        dbName,
                                                        rawKVEntry,
                                                        tableInfoOptional.get());
                                        // todo
                                        eventListener.notify(polymorphicEvent);
                                    }
                                }
                            },
                            clientExecutor);
            completableFutures.add(completableFuture);
        }
        // add a shutdown hook to trigger the stop the process
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    public long getResolvedTs() {
        return resolvedTs.get();
    }

    public Optional<TiTableInfo> getTableInfo(String dbName, String tableName) {
        return Optional.ofNullable(this.tiSession.getCatalog().getTable(dbName, tableName));
    }

    public void addListener(EventListener listener) {
        this.listeners.add(listener);
    }

    public boolean isRunning() {
        return started.get();
    }

    public void join() {
        if (started.get()) {
            completableFutures.forEach(
                    cf -> {
                        CompletableFuture.allOf(cf).join();
                    });
        }
    }

    public void close() {
        try {
            // client stopped.
            started.set(false);
            // shutdown executor.
            clientExecutor.shutdown();
            long clientClosedTimeoutSeconds = 30L;
            if (!clientExecutor.awaitTermination(clientClosedTimeoutSeconds, TimeUnit.SECONDS)) {
                LOGGER.error(
                        "Failed to close the cdc client in {} seconds.",
                        clientClosedTimeoutSeconds);
            }
        } catch (InterruptedException e) {
            LOGGER.error("Closed Client exception.", e);
            throw new ClientException(e);
        }
    }

    private List<RegionStateManager.SingleRegionInfo> divideToRegions(KeyRange keyRange) {
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
                    // todo devide span to paritial span;
                    KeyRange regionKeyRange =
                            KeyRange.newBuilder()
                                    .setStart(tiRegion.getStartKey())
                                    .setEnd(tiRegion.getEndKey())
                                    .build();
                    RPCContext rpcContext =
                            new RPCContext.Builder()
                                    .setRegion(tiRegion.getVerID())
                                    .setAddress(address)
                                    .setMeta(tiRegion.getMeta())
                                    .setChannel(this.tiSession.getChannelFactory())
                                    .setHostMapping(this.tiSession.getPDClient().getHostMapping())
                                    .setPeer(tiRegion.getLeader())
                                    .setTiStore(
                                            this.tiSession
                                                    .getRegionManager()
                                                    .getStoreById(
                                                            tiRegion.getLeader().getStoreId()))
                                    .build();

                    RegionStateManager.SingleRegionInfo signalRegionInfo =
                            new RegionStateManager.SingleRegionInfo(
                                    tiRegion.getVerID(), regionKeyRange, rpcContext);
                    singleRegionInfos.add(signalRegionInfo);
                });
        return singleRegionInfos;
    }

    private void requestRegionToStore(
            RegionStateManager.SingleRegionInfo sri, final long startTs, long tableId) {
        long requestId = IDAllocator.allocateRequestID();
        Cdcpb.Header header =
                Cdcpb.Header.newBuilder()
                        .setTicdcVersion(TiDBVersion.V6_5.getVersion())
                        .setClusterId(this.tiSession.getPDClient().getClusterId())
                        .build();

        final Cdcpb.ChangeDataRequest request =
                Cdcpb.ChangeDataRequest.newBuilder()
                        .setRequestId(requestId)
                        .setHeader(header)
                        .setRegionId(sri.getRpcCtx().getRegion().getId())
                        .setCheckpointTs(startTs)
                        .setStartKey(sri.getRpcCtx().getMeta().getStartKey())
                        .setEndKey(sri.getRpcCtx().getMeta().getEndKey())
                        .setRegionEpoch(sri.getRpcCtx().getMeta().getRegionEpoch())
                        .setExtraOp(Kvrpcpb.ExtraOp.ReadOldValue)
                        .setFilterLoop(true)
                        .build();
        String storeAddr = sri.getRpcCtx().getAddress();
        long storeId = sri.getRpcCtx().getTiStore().getId();
        EventFeedStream streamClient = storeStreamCache.get(storeAddr);
        if (!storeStreamCache.containsKey(storeAddr) || streamClient.getIsCanceled()) {
            if (storeStreamCache.containsKey(storeAddr)) {
                deleteStream(streamClient);
            }
            EventFeedStream stream =
                    new EventFeedStream(storeAddr, storeId, sri.getRpcCtx().getChannel());
            storeStreamCache.put(storeAddr, stream);
            LOGGER.info(
                    "creating new stream {} to store {} to send request",
                    stream.getStreamId(),
                    storeAddr);
            streamClient = stream;
        }
        RegionStateManager.RegionFeedState state =
                new RegionStateManager.RegionFeedState(sri, requestId);
        streamClient.getRegions().setByRequestID(requestId, state);
        try {
            receiveFromStream(streamClient, request, tableId);
            LOGGER.debug(
                    "start new request.tableID {},regionID {}, storeAdd {}",
                    tableId,
                    sri.getRpcCtx().getRegion().getId(),
                    storeAddr);
        } catch (Exception e) {
            try {
                streamClient.close();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            // todo sendRequestToStoreError.
            // Delete the stream from the cache so that when next time a region of
            // this store is requested, a new stream to this store will be created.
            deleteStream(streamClient);
            // Remove the region from pendingRegions. If it's already removed, it should be already
            // retried
            // by `receiveFromStream`, so no need to retry here.
            streamClient.getRegions().takeByRequestID(requestId);
        }
    }

    private void receiveFromStream(
            EventFeedStream stream, Cdcpb.ChangeDataRequest request, long tableId) {
        //    tableStoreStats.lock();
        String key = String.format("%d_%s", tableId, stream.getStoreId());
        if (!tableStoreStats.containsKey(key)) {
            tableStoreStats.put(key, new TableStoreStat());
        }

        RegionWorker worker = new RegionWorker(tiSession, stream, eventConsumer, cdcConfig);
        StreamObserver<Cdcpb.ChangeDataEvent> responseObserver =
                new StreamObserver<Cdcpb.ChangeDataEvent>() {
                    long maxCommitTs = 0L;

                    @Override
                    public void onNext(Cdcpb.ChangeDataEvent event) {
                        long size = event.getSerializedSize();
                        if (size > cdcConfig.getMaxRowKeySize()) {
                            int regionCount = 0;
                            if (event.hasResolvedTs()) {
                                regionCount = event.getResolvedTs().getRegionsCount();
                            }
                            LOGGER.warn(
                                    "change data event size too large. size:{},resolvedRegionCount:{}",
                                    size,
                                    regionCount);
                        }
                        if (!event.getEventsList().isEmpty()) {
                            if (event.getEventsList().get(0).hasEntries()) {
                                long commitTs =
                                        event.getEventsList()
                                                .get(0)
                                                .getEntries()
                                                .getEntries(0)
                                                .getCommitTs();
                                if (maxCommitTs < commitTs) {
                                    maxCommitTs = commitTs;
                                }
                            }
                        }

                        sendRegionChangeEvent(event.getEventsList(), worker);
                        if (event.hasResolvedTs()) {
                            sendResolveTs(event.getResolvedTs(), worker);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        // kvClientStreamRevError.
                        LOGGER.error("kvClientStreamRevError.", t);
                    }

                    @Override
                    public void onCompleted() {
                        LOGGER.warn("Server completed streaming");
                    }
                };
        BackOffer bo = ConcreteBackOffer.newCustomBackOff(60000, stream.getStreamId());
        StreamObserverAdapter.CallFactory<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
                callFactory = (observer) -> stream.getAsyncStub().eventFeed(observer);
        StreamObserverAdapter<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent> adapter =
                new StreamObserverAdapter<>(bo, callFactory);
        StreamObserverAdapter<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>.RetryStreamObserver
                requestObserver = adapter.start(responseObserver);
        requestObserver.sendRequest(request);
    }

    private void deleteStream(EventFeedStream deleteStreamClient) {
        EventFeedStream regionStreamClientInMap =
                storeStreamCache.get(deleteStreamClient.getAddr());
        if (regionStreamClientInMap == null) {
            LOGGER.warn(
                    "Delete stream {} failed, stream not found,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (regionStreamClientInMap.getStreamId() != deleteStreamClient.getStreamId()) {
            LOGGER.warn(
                    "Delete stream {} failed, stream id mismatch,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (Duration.between(deleteStreamClient.getCreateTime(), Instant.now()).getSeconds() < 1) {
            LOGGER.warn(
                    "It's too soon to delete a stream {}, wait for a while,sinceCreateDuration {}",
                    deleteStreamClient.getStreamId(),
                    deleteStreamClient.getCreateTime());
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                LOGGER.error("InterruptedException", e);
            }
        }
        try {
            deleteStreamClient.close();
        } catch (Exception e) {
            LOGGER.error("Region stream client {} closed failed.", deleteStreamClient.getAddr(), e);
            throw new RuntimeException(e);
        }
        storeStreamCache.remove(deleteStreamClient.getAddr());
        LOGGER.info(
                "Region stream client id {}, storeId {} has been removed.",
                deleteStreamClient.getStreamId(),
                deleteStreamClient.getAddr());
    }

    private void sendRegionChangeEvent(List<Cdcpb.Event> events, RegionWorker worker) {
        List<List<RegionStatefulEvent>> regionStatefulEeventList =
                IntStream.range(0, worker.getWorkerConcurrency())
                        .mapToObj(
                                i ->
                                        IntStream.range(0, events.size())
                                                .mapToObj(
                                                        j ->
                                                                new RegionStatefulEvent()) // 假设有一个无参构造函数
                                                .collect(Collectors.toList()))
                        .collect(Collectors.toList());
        int totalEvents = events.size();
        for (int i = 0; i < worker.getWorkerConcurrency(); i++) {
            // Calculate buffer length as 1.5 times the average number of events per worker
            int buffLen = totalEvents / worker.getWorkerConcurrency() * 3 / 2;
            regionStatefulEeventList.add(new ArrayList<>(buffLen));
        }
        for (Cdcpb.Event event : events) {
            RegionStateManager.RegionFeedState state = worker.getRegionState(event.getRegionId());
            //      boolean valid = true;
            if (state != null) {
                if (state.getRequestID() < event.getRequestId()) {
                    LOGGER.debug(
                            "region state entry will be replaced because received message of newer requestID.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                } else if (state.getRequestID() > event.getRequestId()) {
                    LOGGER.debug(
                            "drop event due to event belongs to a stale request.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
                if (state.isStale()) {
                    LOGGER.warn(
                            "drop event due to region feed is stopped.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
            } else {
                // Firstly load the region info.
                RegionStateManager.RegionFeedState newState =
                        worker.getStream().getRegions().takeByRequestID(event.getRequestId());
                if (newState == null) {
                    LOGGER.warn(
                            "drop event due to region feed is removed.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
                newState.start();
                state = newState;
                worker.setRegionState(event.getRegionId(), newState);
            }
            if (event.hasError()) {
                LOGGER.error(
                        "event feed receives a region error.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                        event.getRegionId(),
                        state.getRequestID(),
                        event.getRegionId(),
                        worker.getStream().getAddr(),
                        worker.getStream().getStreamId());
            }
            int slot = worker.inputCalcSlot(event.getRegionId());
            // build stateful event;
            regionStatefulEeventList
                    .get(slot)
                    .add(
                            new RegionStatefulEvent.Builder()
                                    .setEvent(event)
                                    .setRegionFeedState(state)
                                    .build());
        }
        for (List<RegionStatefulEvent> rsevents : regionStatefulEeventList) {
            if (rsevents.size() > 0) {
                worker.processEvents(rsevents);
            }
        }
    }

    private void sendResolveTs(Cdcpb.ResolvedTs resolvedTs, RegionWorker worker) {
        List<RegionStatefulEvent> regionStatefulEvents =
                IntStream.range(0, worker.getWorkerConcurrency())
                        .mapToObj(i -> new RegionStatefulEvent()) // 假设有一个无参构造函数
                        .collect(Collectors.toList());
        for (int i = 0; i < worker.getWorkerConcurrency(); i++) {
            int buffLen = resolvedTs.getRegionsList().size() / worker.getWorkerConcurrency() * 2;
            RegionStatefulEvent rse = this.resolveTsPool.poll();
            if (rse == null) {
                rse = new RegionStatefulEvent();
                this.resolveTsPool.add(rse);
            }
            rse.getResolvedTsEvent().setResolvedTs(resolvedTs.getTs());
            rse.getResolvedTsEvent().setRegions(new ArrayList<>(buffLen));
            regionStatefulEvents.set(i, rse);
        }
        for (long regionID : resolvedTs.getRegionsList()) {
            RegionStateManager.RegionFeedState state = worker.getRegionState(regionID);
            if (state != null) {
                int slot = worker.inputCalcSlot(regionID);
                regionStatefulEvents.get(slot).getResolvedTsEvent().getRegions().add(state);
                regionStatefulEvents.get(slot).setRegionId(regionID);
            }
        }
        for (RegionStatefulEvent rse : regionStatefulEvents) {
            if (rse.getResolvedTsEvent().getRegions().size() > 0) {
                worker.processEvents(Lists.newArrayList(rse));
            }
        }
    }
}
