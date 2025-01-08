package org.tikv.cdc.kv;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.shaded.netty4.io.netty.util.concurrent.OrderedEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.shade.io.grpc.CallOptions;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.MethodDescriptor;
import org.tikv.shade.io.grpc.Status;
import org.tikv.shade.io.grpc.internal.SerializingExecutor;
import org.tikv.shade.io.grpc.stub.ClientCallStreamObserver;
import org.tikv.shade.io.grpc.stub.ClientCalls;
import org.tikv.shade.io.grpc.stub.ClientResponseObserver;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class GRPCClient {
    private static final Logger LOG = LoggerFactory.getLogger(GRPCClient.class);
    private final ManagedChannel channel;
    // limit overall rate of immediate retries after failed calls to 1/sec,
    // to avoid excessive requests when there are connection problems
    protected final RateLimiter immediateRetryLimiter = RateLimiter.create(1.0);

    // modified only by reauthenticate() method
    private CallOptions callOptions = CallOptions.DEFAULT; // volatile tbd - lazy probably ok

    protected final ScheduledExecutorService ses;

    protected final Executor userExecutor;

    public GRPCClient(ManagedChannel channel, ScheduledExecutorService ses, Executor userExecutor) {
        this.channel = Preconditions.checkNotNull(channel, "channel");
        this.ses = ses;
        this.userExecutor = userExecutor;
    }

    protected CallOptions getCallOptions() {
        return callOptions;
    }

    public static boolean causedBy(Throwable t, Class<? extends Throwable> exClass) {
        return t != null
                && (exClass.isAssignableFrom(t.getClass()) || causedBy(t.getCause(), exClass));
    }

    public static boolean isConnectException(Throwable t) {
        return causedBy(t, ConnectException.class) || causedBy(t, NoRouteToHostException.class);
    }

    public static Status.Code codeFromThrowable(Throwable t) {
        return Status.fromThrowable(t).getCode(); // fromThrowable won't return null
    }

    protected boolean retryableStreamError(Throwable error) {
        return (Status.fromThrowable(error).getCode() != Status.Code.INVALID_ARGUMENT
                && !causedBy(error, Error.class));
    }

    public Executor getResponseExecutor() {
        return userExecutor;
    }

    /** Care should be taken not to use this executor for any blocking or CPU intensive tasks. */
    public ScheduledExecutorService getInternalExecutor() {
        return ses;
    }

    private static final Class<? extends Executor> GSE_CLASS =
            MoreExecutors.newSequentialExecutor(directExecutor()).getClass();

    public static Executor serialized(Executor parent) {
        return parent instanceof SerializingExecutor
                        || parent instanceof SerializingExecutor
                        || parent instanceof OrderedEventExecutor
                        || parent.getClass() == GSE_CLASS
                ? parent
                : new SerializingExecutor(parent);
    }

    public <ReqT, RespT> StreamObserver<ReqT> callStream(
            MethodDescriptor<ReqT, RespT> method,
            ResilientResponseObserver<ReqT, RespT> respStream,
            Executor responseExecutor) {
        return new ResilientBiDiStream<>(method, respStream, responseExecutor).start();
    }

    public interface ResilientResponseObserver<ReqT, RespT> extends StreamObserver<RespT> {
        /**
         * Called once initially, and once after each {@link #onReplaced(StreamObserver)}, to
         * indicate the corresponding (sub) stream is successfully established
         */
        void onEstablished();

        /**
         * Indicates the underlying stream failed and will be re-established. There is no guarantee
         * that any requests sent to the current request stream have been delivered, the provided
         * stream should be used in its place to send all subsequent requests, including
         * re-submissions if necessary. Any subsequent {@link #onEstablished()} or {@link
         * #onNext(Object)} calls received will be responses from this <b>new</b> stream, it's
         * guaranteed that there will be no more from the prior stream.
         *
         * @param newStreamRequestObserver
         */
        void onReplaced(StreamObserver<ReqT> newStreamRequestObserver);
    }

    @SuppressWarnings("rawtypes")
    private static final StreamObserver<?> EMPTY_STREAM =
            new StreamObserver() {
                @Override
                public void onCompleted() {}

                @Override
                public void onError(Throwable t) {}

                @Override
                public void onNext(Object value) {}
            };

    protected static void closeStream(StreamObserver<?> stream, Throwable err) {
        if (err == null) {
            stream.onCompleted();
        } else {
            stream.onError(err);
        }
    }

    @SuppressWarnings("unchecked")
    protected static <ReqT> StreamObserver<ReqT> emptyStream() {
        return (StreamObserver<ReqT>) EMPTY_STREAM;
    }

    final class ResilientBiDiStream<ReqT, RespT> {
        private final MethodDescriptor<ReqT, RespT> method;
        private final ResilientResponseObserver<ReqT, RespT> respStream;
        private final Executor responseExecutor;

        // null if !sendViaEventLoop
        private final Executor requestExecutor;

        // accessed only from response thread and retry task scheduled
        // from the onError message (prior to stream being reestablished)
        private CallOptions sentCallOptions;
        private int errCounter = 0;

        // provided to user, buffers and wraps real req stream when active.
        // field accessed only from response thread
        private RequestSubStream userReqStream;

        // finished reflects *user* closing stream via terminal method (not incoming stream closure)
        // error == null indicates complete versus failed when finished == true
        // modified only by response thread
        private boolean finished;
        private Throwable error;

        /**
         * @param method
         * @param respStream
         */
        ResilientBiDiStream(
                MethodDescriptor<ReqT, RespT> method,
                ResilientResponseObserver<ReqT, RespT> respStream,
                Executor responseExecutor) {
            this.method = method;
            this.respStream = respStream;
            this.responseExecutor = responseExecutor;
            this.requestExecutor = ses;
        }

        // must only be called once - enforcement logic omitted since private
        StreamObserver<ReqT> start() {
            RequestSubStream firstStream = new RequestSubStream();
            userReqStream = firstStream;
            responseExecutor.execute(this::refreshBackingStream);
            return firstStream;
        }

        class RequestSubStream implements StreamObserver<ReqT> {
            // lifecycle: null -> real stream -> EMPTY_STREAM
            private volatile StreamObserver<ReqT> grpcReqStream; // only modified by response thread
            // grpcReqStream non-null => preConnectBuffer null
            private Queue<ReqT> preConnectBuffer;

            // called by user thread
            @Override
            public void onNext(ReqT value) {
                if (finished) {
                    return; // illegal usage
                }
                StreamObserver<ReqT> rs = grpcReqStream;
                if (rs == null)
                    synchronized (this) {
                        rs = grpcReqStream;
                        if (rs == null) {
                            if (preConnectBuffer == null) {
                                preConnectBuffer = new ArrayDeque<>(8); // bounded TBD
                            }
                            preConnectBuffer.add(value);
                            return;
                        }
                    }

                if (requestExecutor == null) {
                    sendOnNext(rs, value); // (***)
                } else {
                    final StreamObserver<ReqT> rsFinal = rs;
                    requestExecutor.execute(() -> sendOnNext(rsFinal, value));
                }
            }

            private void sendOnNext(StreamObserver<ReqT> reqStream, ReqT value) {
                try {
                    reqStream.onNext(value);
                } catch (IllegalStateException ise) {
                    // this is possible and ok if the stream was already closed
                    if (grpcReqStream != emptyStream()) throw ise;
                }
            }

            // called by user thread
            @Override
            public void onError(Throwable t) {
                onFinish(t);
            }

            // called by user thread
            @Override
            public void onCompleted() {
                onFinish(null);
            }

            // called from response thread
            boolean established(StreamObserver<ReqT> stream) {
                StreamObserver<ReqT> curStream = grpcReqStream;
                if (curStream == null)
                    synchronized (this) {
                        Queue<ReqT> pcb = preConnectBuffer;
                        if (pcb != null) {
                            for (ReqT req; (req = pcb.poll()) != null; ) {
                                stream.onNext(req);
                            }
                            preConnectBuffer = null;
                        }
                        initialReqStream = null;
                        if (finished) {
                            grpcReqStream = emptyStream();
                        } else {
                            grpcReqStream = stream;
                            return true;
                        }
                    }
                else if (stream == curStream) {
                    return false;
                }

                // here either finished or it's an unexpected new stream
                if (!finished) {
                    LOG.info(
                            "Closing unexpected new stream of method {}", method.getFullMethodName());
                }
                closeStream(stream, error);
                return false;
            }

            boolean isEstablished() {
                return grpcReqStream != null;
            }

            // called by user thread
            private void onFinish(Throwable err) {
                if (finished) {
                    return; // shouldn't be called more than once anyhow
                }
                responseExecutor.execute(
                        () -> {
                            if (finished) {
                                return;
                            }
                            if (err == null) {
                                error = err;
                                finished = true;
                            }
                            userReqStream.close(err, true);
                        });
            }

            // called from grpc response thread
            void discard(Throwable err) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if (curStream == empty) {
                    return;
                }
                if (curStream == null)
                    synchronized (this) {
                        grpcReqStream = empty;
                        preConnectBuffer = null;
                    }
                else {
                    // TODO this *could* overlap with an in-progress
                    //   onNext (***) above in the sendViaEventLoop == false case, but unlikely
                    // For now, delay sending the close to further minimize the chance
                    close(err, false);
                }
            }

            // called from grpc response thread
            void close(Throwable err, boolean fromUser) {
                StreamObserver<ReqT> curStream = grpcReqStream, empty = emptyStream();
                if (curStream == null || curStream == empty) {
                    return;
                }
                grpcReqStream = empty;
                if (fromUser) {
                    closeStream(curStream, err);
                } else {
                    Runnable closeTask = () -> closeStream(curStream, err);
                    if (requestExecutor != null) {
                        requestExecutor.execute(closeTask);
                    } else {
                        ses.schedule(closeTask, 400, MILLISECONDS);
                    }
                }
            }
        }
        /*
         * We assume the caller (grpc) abides by StreamObserver contract
         */
        private final StreamObserver<RespT> respWrapper =
                new ClientResponseObserver<ReqT, RespT>() {
                    @Override
                    public void beforeStart(ClientCallStreamObserver<ReqT> rs) {
                        rs.setOnReadyHandler(
                                () -> {
                                    // called from grpc response thread
                                    if (rs.isReady()) {
                                        errCounter = 0;
                                        boolean notify = userReqStream.established(rs);
                                        if (notify) {
                                            respStream.onEstablished();
                                        }
                                    }
                                });
                    }
                    // called from grpc response thread
                    @Override
                    public void onNext(RespT value) {
                        respStream.onNext(value);
                    }
                    // called from grpc response thread
                    @Override
                    public void onError(Throwable t) {
                        boolean finalError;
                        if (finished) {
                            finalError = true;
                        } else {
                            finalError = !retryableStreamError(t);
                        }
                        if (!finalError) {
                            int errCount;

                            errCount = ++errCounter;
                            LOG.error(
                                    "Retryable onError #{} on underlying stream of method {}",
                                    errCount,
                                    method.getFullMethodName(),
                                    t);

                            RequestSubStream userStreamBefore = userReqStream;
                            if (userStreamBefore.isEstablished()) {
                                userReqStream = new RequestSubStream();
                                userStreamBefore.discard(null);

                                // must call onReplaced prior to refreshing the stream, otherwise
                                // the response observer may be called with responses from the
                                // new stream prior to onReplaced returning
                                respStream.onReplaced(userReqStream);
                            } else if (initialReqStream != null) {
                                // else no need to replace user stream, but cancel outbound stream
                                initialReqStream.onError(t);
                                initialReqStream = null;
                            }

                            // re-attempt immediately
                            if (errCount <= 1 && immediateRetryLimiter.tryAcquire()) {
                                refreshBackingStream();
                            } else {
                                // delay stream retry using backoff/jitter strategy
                                ses.schedule(
                                        ResilientBiDiStream.this::refreshBackingStream,
                                        // skip attempt in rate-limited case (errCount <=1)
                                        delayAfterFailureMs(Math.max(errCount, 2)),
                                        MILLISECONDS);
                            }
                        } else {
                            sentCallOptions = null;
                            userReqStream.discard(t);
                            respStream.onError(t);
                        }
                    }
                    // called from grpc response thread
                    @Override
                    public void onCompleted() {
                        if (!finished) {
                            LOG.warn(
                                    "Unexpected onCompleted received for stream of method {}",method.getFullMethodName());
                            // TODO(maybe) reestablish stream in this case?
                        }
                        sentCallOptions = null;
                        userReqStream.discard(null);
                        respStream.onCompleted();
                    }
                };
        // called only from:
        // - grpc response thread
        // - scheduled retry (no active stream)
        private void refreshBackingStream() {
            if (finished) {
                return;
            }
            LOG.debug("Refreshing backing stream");
            CallOptions callOpts = getCallOptions();
            sentCallOptions = callOpts;
            callOpts = callOpts.withExecutor(responseExecutor);
            initialReqStream =
                    ClientCalls.asyncBidiStreamingCall(
                            channel.newCall(method, callOpts), respWrapper);
        }

        // this is just stored to cancel if the call fails before
        // being established
        private StreamObserver<ReqT> initialReqStream;
    }
    /** @param failedAttemptNumber number of the attempt which just failed, 1-based */
    static long delayAfterFailureMs(int failedAttemptNumber) {
        // backoff delay pattern: 0, [500ms - 1sec), 2sec, 4sec, 8sec, 8sec, ... (jitter after first retry)
        if (failedAttemptNumber <= 1) {
            return 0L;
        }
        return failedAttemptNumber == 2
                ? 500L + ThreadLocalRandom.current().nextLong(500L)
                : (2000L << Math.min(failedAttemptNumber - 3, 2));
    }
}
