package org.tikv.cdc.kv;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.util.IDAllocator;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.MethodDescriptor;
import org.tikv.shade.io.grpc.stub.StreamObserver;
import org.tikv.shade.io.netty.channel.MultithreadEventLoopGroup;
import org.tikv.shade.io.netty.channel.epoll.Epoll;
import org.tikv.shade.io.netty.channel.epoll.EpollEventLoopGroup;
import org.tikv.shade.io.netty.channel.nio.NioEventLoopGroup;

import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class EventFeedStream implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(EventFeedStream.class);
    private static final Pattern CANCEL_REASON_PATT =
            Pattern.compile("rpc error: code = (\\w+) desc = (.*)");

    private static final MethodDescriptor<Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
            METHOD_EVENT_FEED = ChangeDataGrpc.getEventFeedMethod();
    private static final Exception CANCEL_EXCEPTION = new CancellationException();

    private final GRPCClient client;

    private final String storeAddr;
    private final Long storeId;
    private final long streamId;
    private final Instant createTime;

    private final RegionStateManager.SyncRegionFeedStateMap regions;

    private final Executor observerExecutor; // "parent" executor
    private final Executor eventLoop; // serialized
    private AtomicReference<ReceiveEvent> receiveEvent = new AtomicReference<>();

    protected boolean closed;

    private StreamObserver<Cdcpb.ChangeDataRequest> requestStream;

    public EventFeedStream(String storeAddr, long storeId, ManagedChannel channel) {
        this.client = buildGrpcClient(channel);
        this.storeAddr = storeAddr;
        this.storeId = storeId;
        this.streamId = IDAllocator.allocateStreamClientID();
        this.regions = new RegionStateManager.SyncRegionFeedStateMap();
        this.createTime = Instant.now();
        this.observerExecutor =this.client.getResponseExecutor();
        this.eventLoop = GRPCClient.serialized(client.getInternalExecutor());
    }

    public boolean StartReceiver(
            Cdcpb.ChangeDataRequest request, StreamObserver<Cdcpb.ChangeDataEvent> observer) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        final ReceiveEvent rEvent = new ReceiveEvent(request, observer, observerExecutor);
        Cdcpb.ChangeDataRequest createReq = rEvent.firstCreateChangeDateRequest();
        synchronized (this) {
            StreamObserver<Cdcpb.ChangeDataRequest> requestStream = getRequestStream();
            if (requestStream == null) {
                return false;
            }
            LOG.info("Send change request to {}", createReq);
            requestStream.onNext(createReq);
            this.receiveEvent.getAndSet(rEvent);
        }
        return true;
    }

    final class ReceiveEvent {
        private final StreamObserver<Cdcpb.ChangeDataEvent> observer;
        private final Cdcpb.ChangeDataRequest request;
        private final Executor receiveExecutor;
        boolean finished;

        private final AtomicLong currentCheckpointTs = new AtomicLong();

        ReceiveEvent(
                Cdcpb.ChangeDataRequest request,
                StreamObserver<Cdcpb.ChangeDataEvent> observer,
                Executor parentExecutor) {
            this.observer = observer;
            this.request = request;
            long rev = request.getRequestId();
            // bounded for back-pressure
            this.receiveExecutor = GRPCClient.serialized(parentExecutor);
        }

        public Cdcpb.ChangeDataRequest firstCreateChangeDateRequest() {
            return request;
        }

        public Cdcpb.ChangeDataRequest newCreateChangeDateRequest() {
            return request.toBuilder()
                    .setRequestId(IDAllocator.allocateRequestID())
                    .setCheckpointTs(this.currentCheckpointTs.get())
                    .build();
        }

        // null => cancelled (non-error)
        public void publishCompletionEvent(final Exception err) {
            receiveExecutor.execute(
                    () -> {
                        try {
                            if (err == null) {
                                observer.onCompleted();
                            } else {
                                observer.onError(err);
                            }
                        } catch (RuntimeException e) {
                            LOG.warn("Receive observer onCompleted/onError threw", e);
                        }
                    });
        }

        public void processEvent(final Cdcpb.ChangeDataEvent event) {
            receiveExecutor.execute(
                    () -> {
                        observer.onNext(event);
                        if (!event.getEventsList().isEmpty()) {
                            if (event.getEventsList().get(0).hasEntries()) {
                                long commitTs =
                                        event.getEventsList()
                                                .get(0)
                                                .getEntries()
                                                .getEntries(0)
                                                .getCommitTs();
                                if (currentCheckpointTs.get() < commitTs) {
                                    currentCheckpointTs.getAndSet(commitTs);
                                }
                            }
                        }
                    });
        }
    }

    public StreamObserver<Cdcpb.ChangeDataRequest> getRequestStream() {
        if (closed) return null;
        if (requestStream == null) {
            LOG.debug("Watch stream starting");
            requestStream = client.callStream(METHOD_EVENT_FEED, responseObserver, eventLoop);
        }
        return requestStream;
    }

    public void closeRequestStreamIfNoEvents() {
        synchronized (this) {
            if (requestStream != null) {
                requestStream.onError(CANCEL_EXCEPTION);
                LOG.info("Watch stream cancelled due to there being no active watches");
                requestStream = null;
            }
        }
    }

    protected final GRPCClient.ResilientResponseObserver<
                    Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>
            responseObserver =
                    new GRPCClient.ResilientResponseObserver<
                            Cdcpb.ChangeDataRequest, Cdcpb.ChangeDataEvent>() {

                        @Override
                        public void onNext(Cdcpb.ChangeDataEvent event) {
                            processResponse(event);
                        }

                        @Override
                        public void onError(Throwable t) {
                            LOG.debug("onError called for watch request stream", t);
                            if (closed || GRPCClient.causedBy(t, CancellationException.class)) {
                                return;
                            }
                            synchronized (EventFeedStream.this) {
                                if (closed) {
                                    return;
                                }
                            }
                            onReplacedOrFailed(
                                    null,
                                    t instanceof Exception
                                            ? (Exception) t
                                            : new RuntimeException(t));
                        }

                        @Override
                        public void onCompleted() {
                            LOG.debug("onCompleted called for watch request stream");
                        }

                        @Override
                        public void onEstablished() {
                            // nothing to do here
                            LOG.debug("onEstablished called for watch request stream");
                        }

                        @Override
                        public void onReplaced(
                                StreamObserver<Cdcpb.ChangeDataRequest> newStreamRequestObserver) {
                            if (!closed) {
                                LOG.info(
                                        "onReplaced called for watch request stream {}",
                                        (newStreamRequestObserver == null
                                                ? " with newReqStream == null"
                                                : ""));
                            }
                            onReplacedOrFailed(newStreamRequestObserver, null);
                        }

                        void onReplacedOrFailed(
                                StreamObserver<Cdcpb.ChangeDataRequest> newReqStream,
                                Exception err) {
                            List<ReceiveEvent> pending = new ArrayList<>();
                            synchronized (EventFeedStream.this) {
                                requestStream = newReqStream;
                                if (receiveEvent.get() != null) {
                                    pending.add(receiveEvent.get());
                                    receiveEvent.set(null);
                                }
                            }
                            boolean isExist = false;
                            for (ReceiveEvent rEvent : pending) {
                                if (rEvent.finished) {
                                    continue;
                                }
                                if (newReqStream != null) {
                                    Cdcpb.ChangeDataRequest newReq =
                                            rEvent.newCreateChangeDateRequest();
                                    synchronized (EventFeedStream.this) {
                                        if (!closed) {
                                            requestStream.onNext(newReq);
                                            receiveEvent.getAndSet(rEvent);
                                            isExist = true;
                                        }
                                    }
                                }
                                if (newReqStream == null || closed) {
                                    rEvent.finished = true;
                                    rEvent.publishCompletionEvent(err);
                                }
                            }
                            if (!isExist) {
                                closeRequestStreamIfNoEvents();
                            }
                        }
                    };

    protected void processResponse(Cdcpb.ChangeDataEvent event) {
        ReceiveEvent re = this.receiveEvent.get();
        if (re == null) {
            LOG.error("State error: received unexpected watch create response: " + event);
            closeRequestStreamIfNoEvents();
            return;
        }
        re.processEvent(event);
    }

    public boolean getIsCanceled() {
        return !closed;
    }

    private GRPCClient buildGrpcClient(ManagedChannel channel) {
        ThreadFactory tfac =
                new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setThreadFactory(CDCClient.EventThread::new)
                        .setNameFormat("event-pool-%d")
                        .build();
        MultithreadEventLoopGroup internalExecutor;
        if (Epoll.isAvailable()) {
            // todo
            internalExecutor = new EpollEventLoopGroup(4, tfac);
        } else {
            internalExecutor = new NioEventLoopGroup(4, tfac);
        }
        Executor userExecutor =
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("callback-thread-%d")
                                .build());

        return new GRPCClient(channel, internalExecutor, userExecutor);
    }

    public String getAddr() {
        return storeAddr;
    }

    public Long getStoreId() {
        return storeId;
    }

    public long getStreamId() {
        return streamId;
    }

    public RegionStateManager.SyncRegionFeedStateMap getRegions() {
        return regions;
    }

    public Instant getCreateTime() {
        return createTime;
    }

    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        eventLoop.execute(
                () -> {
                    if (!closed)
                        synchronized (EventFeedStream.this) {
                            if (closed) {
                                return;
                            }
                            closed = true;
                            if (requestStream != null) {
                                requestStream.onError(CANCEL_EXCEPTION);
                                requestStream = null;
                            }
                            responseObserver.onReplaced(null);
                        }
                });
    }
}
