/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.service.jobresults.server;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.SerializedExecutor;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.sabot.rpc.ExecToCoordResultsHandler;
import com.dremio.service.Pointer;
import com.dremio.service.grpc.CloseableBindableService;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.service.jobresults.JobResultsResponse;
import com.dremio.service.jobresults.JobResultsServiceGrpc;
import com.dremio.services.jobresults.common.JobResultsRequestWrapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import io.grpc.stub.StreamObserver;
import io.opentracing.Tracer;

/**
 * Job Results gRPC service.
 */
public class JobResultsGrpcServerFacade extends JobResultsServiceGrpc.JobResultsServiceImplBase implements CloseableBindableService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsGrpcServerFacade.class);

  private Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider;
  private final Provider<MaestroForwarder> forwarder;
  private final ExecutorService threadPool;
  private final SerializedExecutor serializedExecutor;

  public JobResultsGrpcServerFacade(Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider,
                                    Provider<MaestroForwarder> forwarder, Tracer tracer) {
    this.execToCoordResultsHandlerProvider = execToCoordResultsHandlerProvider;
    this.forwarder = forwarder;
    ExecutorService threadPoolInternal = new ThreadPoolExecutor(10, Integer.MAX_VALUE,
      60L, TimeUnit.SECONDS,
      new SynchronousQueue<Runnable>());
    threadPool = new ContextMigratingExecutorService<>(threadPoolInternal);
    serializedExecutor = new SerializedExecutor("jobResults", threadPool, false) {
      @Override
      protected void runException(Runnable command, Throwable t) {
        logger.error("exception handling results", t);
      }
    };
  }

  @Override
  public StreamObserver<JobResultsRequest> jobResults(StreamObserver<JobResultsResponse> responseObserver) {

    return new StreamObserver<JobResultsRequest>() {
      private Pointer<String> queryIdProvider = new Pointer<String>(null);

      @Override
      public void onNext(JobResultsRequest request) {
        onNextHandler(request, null, queryIdProvider, responseObserver);
      }

      @Override
      public void onError(Throwable t) {
        onErrorHandler(queryIdProvider.value, t, responseObserver);
      }

      @Override
      public void onCompleted() {
        onCompletedHandler(queryIdProvider.value, responseObserver);
      }
    };
  }

  /*
   * Only one of {@param request} or {@param requestWrapper} should be non-null
   */
  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private void onNextHandler(JobResultsRequest request,
                             JobResultsRequestWrapper requestWrapper,
                             Pointer<String> queryId,
                             StreamObserver<JobResultsResponse> responseObserver) {
    Preconditions.checkArgument(request == null ^ requestWrapper == null,
      "Either request or requestWrapper should be non-null.");
    long sequenceId;
    UserBitShared.QueryData queryHeader;

    if (request != null) {
      queryHeader = request.getHeader();
      sequenceId = request.getSequenceId();
    } else {
      queryHeader = requestWrapper.getHeader();
      sequenceId = requestWrapper.getSequenceId();
    }

    queryId.value = QueryIdHelper.getQueryId(queryHeader.getQueryId());

    try {
      JobResultsGrpcLocalResponseSender sender = new JobResultsGrpcLocalResponseSender(
        responseObserver,
        sequenceId,
        queryId.value);

      // unblock the stream and allow it process the next message since the same request
      // thread is used for all batches in the stream.
      // use a serialized executor to make sure batches are processed in order.
      serializedExecutor.execute( () -> {
        try {
          if (request != null) {
            // When request is received from executor to one of coordinator, forwarding may or may not be needed.
            // By passing "data" as null, we skip populating "data" from request into direct-memory in case
            // request has to be forwarded to foreman coordinator.
            execToCoordResultsHandlerProvider.get().dataArrived(request.getHeader(), null, request, sender);
          } else {
            boolean forwaded = execToCoordResultsHandlerProvider.get().dataArrived(requestWrapper, sender);
            if (!forwaded) {
              // attempt manager has taken a ref to write to client
              // release now to not leak
              requestWrapper.close();
            }
          }
        } catch (RpcException e) {
          Throwables.propagate(e);
        }
      });
    } catch (Exception ex) {
      logger.error("Failed to handle result for queryId {}", queryId, ex);
      responseObserver.onError(ex);
    }
  }

  @SuppressWarnings("DremioGRPCStreamObserverOnError")
  private void onErrorHandler(String queryId, Throwable t, StreamObserver<JobResultsResponse> responseObserver) {
    logger.error("JobResultsService stream failed with error ", t);
    forwarder.get().resultsError(queryId, t);
    responseObserver.onError(t);
  }

  private void onCompletedHandler(String queryId, StreamObserver<JobResultsResponse> responseObserver) {
    forwarder.get().resultsCompleted(queryId);
    responseObserver.onCompleted();
  }

  public StreamObserver<JobResultsRequestWrapper> getRequestWrapperStreamObserver(StreamObserver<JobResultsResponse> responseObserver) {
    return new StreamObserver<JobResultsRequestWrapper>() {
      private Pointer<String> queryIdProvider = new Pointer<String>(null);

      @Override
      public void onNext(JobResultsRequestWrapper requestWrapper) {
        onNextHandler(null, requestWrapper, queryIdProvider, responseObserver);
      }

      @Override
      public void onError(Throwable t) {
        onErrorHandler(queryIdProvider.value, t, responseObserver);
      }

      @Override
      public void onCompleted() {
        onCompletedHandler(queryIdProvider.value, responseObserver);
      }
    };
  }

  @Override
  public void close() throws Exception {
  }

}
