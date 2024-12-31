package org.tikv.cdc.kv;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.util.BackOffer;
import org.tikv.shade.io.grpc.Status;
import org.tikv.shade.io.grpc.StatusRuntimeException;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import static java.lang.Thread.sleep;
import static org.tikv.common.util.BackOffFunction.BackOffFuncType.BoTiKVRPC;

/**
 * retry stream Observer.
 *
 * @param <ReqT>
 * @param <RespT>
 */
public class StreamObserverAdapter<ReqT, RespT> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StreamObserverAdapter.class);
  private final BackOffer backOffer;
  private final CallFactory<ReqT, RespT> callFactory; // Factory for creating gRPC calls

  public StreamObserverAdapter(BackOffer backOffer, CallFactory<ReqT, RespT> callFactory) {
    this.backOffer = backOffer;
    this.callFactory = callFactory;
  }

  public RetryStreamObserver start(StreamObserver<RespT> responseObserver) {
    return new RetryStreamObserver(responseObserver);
  }

  public class RetryStreamObserver implements StreamObserver<RespT> {

    private final StreamObserver<RespT> responseObserver;
    private StreamObserver<ReqT> requestObserver;

    public RetryStreamObserver(StreamObserver<RespT> responseObserver) {
      this.responseObserver = responseObserver;
      initializeCall();
    }

    private void initializeCall() {
      this.requestObserver = callFactory.createCall(this);
    }

    @Override
    public void onNext(RespT value) {
      responseObserver.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      if (!backOffer.canRetryAfterSleep(BoTiKVRPC)) {
        // not retry
        LOGGER.error("Retry is exhaust!");
        requestObserver.onError(t);
        return;
      }
      // sleep
      try {
        sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.error("InterruptedException", e);
      }
      LOGGER.debug("Stream observer is error!");
      if (t instanceof StatusRuntimeException) {
        StatusRuntimeException e = (StatusRuntimeException) t;
        if (Status.Code.UNAVAILABLE.equals(e.getStatus().getCode())
            || Status.Code.DEADLINE_EXCEEDED.equals(e.getStatus().getCode())) {
          SlowLogSpan retryInitializeCall = backOffer.getSlowLog().start("retry_initialize_call");
          try {
            initializeCall(); // Reinitialize the gRPC call
          } finally {
            retryInitializeCall.end();
          }
        } else {
          LOGGER.error("Stream observer is failed.", t);
          return;
        }
      }
      initializeCall(); // Reinitialize the gRPC call
    }

    @Override
    public void onCompleted() {
      responseObserver.onCompleted();
    }

    public void sendRequest(ReqT request) {
      if (requestObserver != null) {
        requestObserver.onNext(request);
      }
    }

    public void complete() {
      if (requestObserver != null) {
        requestObserver.onCompleted();
      }
    }
  }

  // Factory interface for creating gRPC calls
  @FunctionalInterface
  public interface CallFactory<ReqT, RespT> {
    StreamObserver<ReqT> createCall(StreamObserver<RespT> responseObserver);
  }
}
