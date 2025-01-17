package org.tikv.cdc.kv;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.cdc.connectors.tidb.table.utils.TableKeyRangeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.cdc.CDCConfig;
import org.tikv.cdc.TableStoreStat;
import org.tikv.cdc.TableStoreStats;
import org.tikv.cdc.exception.ClientException;
import org.tikv.cdc.frontier.Frontier;
import org.tikv.cdc.frontier.KeyRangeFrontier;
import org.tikv.cdc.model.*;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.event.CacheInvalidateEvent;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.IDAllocator;
import org.tikv.common.util.RangeSplitter;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.stub.StreamObserver;
import org.tikv.shade.io.netty.channel.MultithreadEventLoopGroup;
import org.tikv.shade.io.netty.channel.epoll.Epoll;
import org.tikv.shade.io.netty.channel.epoll.EpollEventLoopGroup;
import org.tikv.shade.io.netty.channel.nio.NioEventLoopGroup;
import org.tikv.shade.io.netty.util.concurrent.FastThreadLocalThread;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class CDCClient {
    private static final Logger LOG = LoggerFactory.getLogger(CDCClient.class);
    private final CDCConfig cdcConfig;
    private final TiSession tiSession;
    private final BlockingQueue<RegionFeedEvent> eventsBuffer;

    private final ConcurrentHashMap<String, EventFeedStream> storeStreamCache =
            new ConcurrentHashMap<>();
    private final ConcurrentLinkedQueue<RegionStatefulEvent> resolveTsPool =
            new ConcurrentLinkedQueue<>();
    private final TableStoreStats tableStoreStats = new TableStoreStats();
    private final AtomicLong resolvedTs = new AtomicLong(1735089007000L << 18);
    private final AtomicLong checkpointTs = new AtomicLong(0);
    private final Consumer<RegionFeedEvent> eventConsumer;
    private final Consumer<RegionErrorInfo> errorInfoConsumer;
    private final List<EventListener> listeners = new ArrayList<>();
    public static final int NO_LEADER_STORE_ID = 0;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final ExecutorService clientExecutor = Executors.newFixedThreadPool(1);
    private final List<CompletableFuture> completableFutures = new ArrayList<>(1);
    private final String dbName;
    private final String tableName;
    private Frontier tsTracker;

    public CDCClient(TiConfiguration tiConf, String dbName, String tableName) {
        this(tiConf, new CDCConfig(), dbName, tableName);
    }

    public CDCClient(TiConfiguration tiConf, CDCConfig cdcConfig, String dbName, String tableName) {
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
                        LOG.error("Events buffer put error!", e);
                    }
                };
        errorInfoConsumer = this::handleError;
    }

    public void start(final long startTs) {
        LOG.debug(
                "Start cdc client at time {},database {} table {} listening.",
                startTs,
                dbName,
                tableName);
        if (started.compareAndSet(false, true)) {
            checkpointTs.getAndSet(startTs);
            CompletableFuture<Void> completableFuture =
                    CompletableFuture.runAsync(
                            () -> { // try insert retry Mechanism
                                Optional<TiTableInfo> tableInfoOptional =
                                        getTableInfo(dbName, tableName);
                                if (!tableInfoOptional.isPresent()) {
                                    LOG.error("Get tableInfo for {}.{} failed!", dbName, tableName);
                                    throw new ClientException(
                                            String.format(
                                                    "Get tableInfo for %s.%s failed.",
                                                    dbName, tableName));
                                }
                                KeyRange keyRange =
                                        TableKeyRangeUtils.getTableKeyRange(
                                                tableInfoOptional.get().getId());

                                // new ts tracker;
                                tsTracker = new KeyRangeFrontier(0, keyRange);
                                List<RegionStateManager.SingleRegionInfo> singleRegionInfos =
                                        divideToRegions(keyRange);
                                for (RegionStateManager.SingleRegionInfo sri : singleRegionInfos) {
                                    requestRegionToStore(sri, tableInfoOptional.get().getId());
                                }
                                while (isRunning()) {
                                    long startTime = Instant.now().toEpochMilli();
                                    Instant lastAdvancedTime = Instant.now();
                                    Instant lastLogSlowRangeTime = Instant.now();
                                    long lastResolvedTs = startTs;
                                    boolean initialized = false;
                                    for (EventListener eventListener : listeners) {
                                        RegionFeedEvent regionFeedEvent = eventsBuffer.poll();
                                        if (regionFeedEvent == null) {
                                            continue;
                                        }
                                        // output.
                                        if (regionFeedEvent.getRawKVEntry() != null) {
                                            checkpointTs.getAndSet(
                                                    regionFeedEvent.getRawKVEntry().getCrts());
                                            eventListener.notify(
                                                    output(
                                                            regionFeedEvent.getRawKVEntry(),
                                                            tableInfoOptional));
                                        }

                                        if (regionFeedEvent.getResolved() != null) {
                                            for (RegionKeyRange kr :
                                                    regionFeedEvent.getResolved().getKeyRanges()) {
                                                // todo comparable
                                                tsTracker.forward(
                                                        kr.getRegionId(),
                                                        kr.getKeyRange(),
                                                        regionFeedEvent
                                                                .getResolved()
                                                                .getResolvedTs());
                                            }
                                            this.resolvedTs.getAndSet(tsTracker.frontier());
                                            if (resolvedTs.get() > 0 && !initialized) {
                                                initialized = true;
                                                LOG.info("puller is initialized.");
                                            }
                                            if (!initialized) {
                                                continue;
                                            }
                                            if (resolvedTs.get() <= lastResolvedTs) {
                                                // todo is stuck
                                            }
                                            lastResolvedTs = resolvedTs.get();
                                            lastAdvancedTime = Instant.now();
                                            output(
                                                    new RawKVEntry.Builder()
                                                            .setCrts(resolvedTs.get())
                                                            .setOpType(OpType.Resolved)
                                                            .setRegionId(
                                                                    regionFeedEvent.getRegionId())
                                                            .build(),
                                                    tableInfoOptional);
                                        }
                                    }
                                }
                            },
                            clientExecutor);
            completableFutures.add(completableFuture);
        }
        // add a shutdown hook to trigger the stop the process
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }

    private PolymorphicEvent output(RawKVEntry raw, Optional<TiTableInfo> tableInfoOptional) {
        if (raw.getCrts() < this.resolvedTs.get()
                || (raw.getCrts() == this.resolvedTs.get()
                        && OpType.Resolved.equals(raw.getOpType()))) {
            LOG.warn(
                    "The crts is fallen back in puller.Schema is {}.{}. ResolveTs is {}",
                    dbName,
                    tableName,
                    this.resolvedTs.get());
            return null;
        }
        return new PolymorphicEvent(
                raw.getStartTs(), raw.getCrts(), dbName, raw, tableInfoOptional.get());
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
                LOG.error(
                        "Failed to close the cdc client in {} seconds.",
                        clientClosedTimeoutSeconds);
            }
        } catch (InterruptedException e) {
            LOG.error("Closed Client exception.", e);
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

    private void requestRegionToStore(RegionStateManager.SingleRegionInfo sri, long tableId) {
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
                        .setCheckpointTs(this.checkpointTs.get())
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
            LOG.info(
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
            LOG.debug(
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
            // retried by `receiveFromStream`, so no need to retry here.
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

        RegionWorker worker =
                new RegionWorker(tiSession, stream, eventConsumer, errorInfoConsumer, cdcConfig);
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
                            LOG.warn(
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
                            sendRegionChangeEvent(event.getEventsList(), worker);
                        }

                        if (event.hasResolvedTs()) {
                            LOG.info(
                                    "Current resolved ts is {},region {}",
                                    event.getResolvedTs().getTs(),
                                    event.getResolvedTs().getRegionsList());
                            sendResolveTs(event.getResolvedTs(), worker);
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        // kvClientStreamRevError.
                        LOG.error("kvClientStreamRevError.", t);
                    }

                    @Override
                    public void onCompleted() {
                        LOG.warn("Server completed streaming");
                    }
                };
        GRPCClient grpcClient = buildGrpcClient(stream.getChannel());
        StreamClient streamClient = new StreamClient(grpcClient);
        boolean started = streamClient.StartReceiver(request, responseObserver);
        LOG.info("Grpc Receiver started status: {}", started);
    }

    private GRPCClient buildGrpcClient(ManagedChannel channel) {
        ThreadFactory tfac =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setThreadFactory(EventThread::new)
                        .setNameFormat("event-pool-%d")
                        .build();
        MultithreadEventLoopGroup internalExecutor;
        if (Epoll.isAvailable()) {
            internalExecutor = new EpollEventLoopGroup(cdcConfig.getWorkerPoolSize(), tfac);
        } else {
            internalExecutor = new NioEventLoopGroup(cdcConfig.getWorkerPoolSize(), tfac);
        }
        Executor userExecutor =
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("callback-thread-%d")
                                .build());

        return new GRPCClient(channel, internalExecutor, userExecutor);
    }

    protected static final class EventThread extends FastThreadLocalThread {
        public EventThread(Runnable r) {
            super(r);
        }
    }

    private void deleteStream(EventFeedStream deleteStreamClient) {
        EventFeedStream regionStreamClientInMap =
                storeStreamCache.get(deleteStreamClient.getAddr());
        if (regionStreamClientInMap == null) {
            LOG.warn(
                    "Delete stream {} failed, stream not found,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (regionStreamClientInMap.getStreamId() != deleteStreamClient.getStreamId()) {
            LOG.warn(
                    "Delete stream {} failed, stream id mismatch,ignore it",
                    deleteStreamClient.getAddr());
            return;
        }
        if (Duration.between(deleteStreamClient.getCreateTime(), Instant.now()).getSeconds() < 1) {
            LOG.warn(
                    "It's too soon to delete a stream {}, wait for a while,sinceCreateDuration {}",
                    deleteStreamClient.getStreamId(),
                    deleteStreamClient.getCreateTime());
            try {
                sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("InterruptedException", e);
            }
        }
        try {
            deleteStreamClient.close();
        } catch (Exception e) {
            LOG.error("Region stream client {} closed failed.", deleteStreamClient.getAddr(), e);
            throw new RuntimeException(e);
        }
        storeStreamCache.remove(deleteStreamClient.getAddr());
        LOG.info(
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
                    LOG.debug(
                            "region state entry will be replaced because received message of newer requestID.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                } else if (state.getRequestID() > event.getRequestId()) {
                    LOG.debug(
                            "drop event due to event belongs to a stale request.regionId {}, oldRequestId {}, requestId{}, add {},streamId {}",
                            event.getRegionId(),
                            state.getRequestID(),
                            event.getRegionId(),
                            worker.getStream().getAddr(),
                            worker.getStream().getStreamId());
                    continue;
                }
                if (state.isStale()) {
                    LOG.warn(
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
                    LOG.warn(
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
                LOG.error(
                        "event feed receives a region error.regionId {}, oldRequestId {}, requestId {}, add {},streamId {}，error is {}",
                        event.getRegionId(),
                        state.getRequestID(),
                        event.getRequestId(),
                        worker.getStream().getAddr(),
                        worker.getStream().getStreamId(),
                        event.getError());
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
            if (!rsevents.isEmpty()) {
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
            if (!rse.getResolvedTsEvent().getRegions().isEmpty()) {
                worker.processEvents(Lists.newArrayList(rse));
            }
        }
    }

    private void handleError(RegionErrorInfo errorInfo) {
        if (errorInfo == null
                || errorInfo.getErrorCode() == null
                || errorInfo.getSingleRegionInfo() == null) {
            LOG.debug("Error info is null.");
            throw new ClientException("receive empty or unknown error msg");
        }
        LOG.error(
                "Error info from Region {}, error info detail: {}",
                errorInfo.getSingleRegionInfo().getVerID().getId(),
                errorInfo.getErrorCode());
        List<RegionStateManager.SingleRegionInfo> sriList = new ArrayList<>();
        if (errorInfo.getErrorCode().hasNotLeader()) {
            long newStoreId = errorInfo.getErrorCode().getNotLeader().getLeader().getStoreId();
            long newRegionId = errorInfo.getErrorCode().getNotLeader().getRegionId();
            TiRegion oldRegion = this.tiSession.getRegionManager().getRegionById(newRegionId);
            LOG.warn(
                    String.format(
                            "NotLeader Error with region id [%d] and old store id [%d], new store id [%d]",
                            oldRegion.getId(), oldRegion.getLeader().getStoreId(), newStoreId));
            if (newStoreId == NO_LEADER_STORE_ID) {
                LOG.info(
                        "Received zero store id, from region [{}] try next time",
                        oldRegion.getId());
                throw new ClientException(
                        String.format(
                                "Received zero store id, from region %d try next time",
                                oldRegion.getId()));
            }
            TiRegion newRegion =
                    this.tiSession.getRegionManager().updateLeader(oldRegion, newStoreId);
            if (newRegion == null) {
                LOG.error(
                        "Invalidate region [{}] cache due to cannot find peer when updating leader.Error info is {}",
                        newRegionId,
                        errorInfo.getErrorCode());
                this.tiSession.getRegionManager().onRequestFail(oldRegion);
                notifyRegionLeaderError(oldRegion);
                return;
            } else {
                // When switch leader fails or the region changed its region epoch,
                // it would be necessary to re-split task's key range for new region.
                if (!oldRegion.getRegionEpoch().equals(newRegion.getRegionEpoch())) {
                    sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan());
                } else {
                    if (oldRegion.getLeader().getStoreId() == newStoreId) {
                        LOG.info(
                                "Ignore store id [{}] has equal error message {}.",
                                oldRegion.getLeader().getStoreId(),
                                errorInfo.getErrorCode());
                    } else {
                        TiStore newStore =
                                this.tiSession.getRegionManager().getStoreById(newStoreId);
                        // update store add.
                        errorInfo.getSingleRegionInfo().getRpcCtx().setTiStore(newStore);
                        String address = newStore.getStore().getAddress();
                        if (newStore.getProxyStore() != null) {
                            address = newStore.getProxyStore().getAddress();
                        }
                        errorInfo.getSingleRegionInfo().getRpcCtx().setAddress(address);
                        LOG.info(
                                "Switch region [{}] to new storeId [{}] to specific leader due to kv return NotLeader.",
                                newRegionId,
                                newRegion.getLeader().getStoreId());
                    }
                }
            }
        } else if (errorInfo.getErrorCode().hasEpochNotMatch()) {
            sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan());
        } else if (errorInfo.getErrorCode().hasRegionNotFound()) {
            sriList = divideToRegions(errorInfo.getSingleRegionInfo().getSpan());
        } else if (errorInfo.getErrorCode().hasDuplicateRequest()) {
            throw new ClientException("Kv client unreachable error.");
        } else if (errorInfo.getErrorCode().hasCompatibility()) {
            throw new ClientException("tikv reported compatibility error, which is not expected.");
        } else if (errorInfo.getErrorCode().hasClusterIdMismatch()) {
            throw new ClientException(
                    "tikv reported the request cluster ID mismatch error, which is not expected.");
        } else {
            TiRegion tiRegion =
                    this.tiSession
                            .getRegionManager()
                            .getRegionById(errorInfo.getSingleRegionInfo().getVerID().getId());
            this.tiSession.getRegionManager().onRequestFail(tiRegion);
            throw new ClientException("Receive empty or unknown error msg.");
        }
        Optional<TiTableInfo> tableInfoOptional = getTableInfo(dbName, tableName);
        LOG.info(
                "Retry to region request. checkpointTs is [{}].",
                this.checkpointTs.getAndIncrement()); // ignore error message

        if (sriList.isEmpty()) {
            sriList.add(errorInfo.getSingleRegionInfo());
        }
        sriList.forEach(
                singleRegionInfo -> {
                    tableInfoOptional.ifPresent(
                            tableInfo -> {
                                requestRegionToStore(
                                        errorInfo.getSingleRegionInfo(), tableInfo.getId());
                            });
                });
    }

    private void notifyRegionLeaderError(TiRegion ctxRegion) {
        notifyRegionRequestError(ctxRegion, 0, CacheInvalidateEvent.CacheType.LEADER);
    }

    private void notifyRegionRequestError(
            TiRegion ctxRegion, long storeId, CacheInvalidateEvent.CacheType type) {
        CacheInvalidateEvent event;
        // When store(region) id is invalid,
        // it implies that the error was not caused by store(region) error.
        switch (type) {
            case REGION:
            case LEADER:
                event = new CacheInvalidateEvent(ctxRegion.getId(), 0, true, false, type);
                break;
            case REGION_STORE:
                event = new CacheInvalidateEvent(ctxRegion.getId(), storeId, true, true, type);
                break;
            case REQ_FAILED:
                event = new CacheInvalidateEvent(0, 0, false, false, type);
                break;
            default:
                throw new IllegalArgumentException("Unexpect invalid cache invalid type " + type);
        }
        if (this.tiSession.getRegionManager().getCacheInvalidateCallbackList() != null) {
            for (Function<CacheInvalidateEvent, Void> cacheInvalidateCallBack :
                    this.tiSession.getRegionManager().getCacheInvalidateCallbackList()) {
                this.tiSession.getRegionManager().getCallBackThreadPool().submit(
                        () -> {
                            try {
                                cacheInvalidateCallBack.apply(event);
                            } catch (Exception e) {
                                LOG.error(String.format("CacheInvalidCallBack failed %s", e));
                            }
                        });
            }
        }
    }
}
