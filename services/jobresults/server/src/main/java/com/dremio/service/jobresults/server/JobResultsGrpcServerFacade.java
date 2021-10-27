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

import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Provider;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.maestro.MaestroForwarder;
import com.dremio.exec.rpc.Acks;
import com.dremio.exec.rpc.Response;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.exec.rpc.UserRpcException;
import com.dremio.sabot.rpc.ExecToCoordResultsHandler;
import com.dremio.service.grpc.CloseableBindableService;
import com.dremio.service.jobresults.JobResultsRequest;
import com.dremio.service.jobresults.JobResultsResponse;
import com.dremio.service.jobresults.JobResultsServiceGrpc;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.NettyArrowBuf;

/**
 * Job Results gRPC service.
 */
public class JobResultsGrpcServerFacade extends JobResultsServiceGrpc.JobResultsServiceImplBase implements CloseableBindableService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobResultsGrpcServerFacade.class);

  private Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider;
  private final BufferAllocator allocator;
  private final Provider<MaestroForwarder> forwarder;

  public JobResultsGrpcServerFacade(Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider,
                                    BufferAllocator allocator,
                                    Provider<MaestroForwarder> forwarder) {
    this.execToCoordResultsHandlerProvider = execToCoordResultsHandlerProvider;
    this.allocator = allocator;
    this.forwarder = forwarder;
  }

  public StreamObserver<JobResultsRequest> jobResults(StreamObserver<JobResultsResponse> responseObserver) {

    return new StreamObserver<JobResultsRequest>() {
      private String queryId;

      @Override
      public void onNext(JobResultsRequest request) {
        try (ArrowBuf buf = allocator.buffer(request.getData().size())) {
          queryId = QueryIdHelper.getQueryId(request.getHeader().getQueryId());
          try {
            ByteBuf dBody = NettyArrowBuf.unwrapBuffer(buf);
            dBody.writeBytes(request.getData().toByteArray());

            execToCoordResultsHandlerProvider.get().dataArrived(request.getHeader(), dBody, request,
              new JobResultsGrpcLocalResponseSender(responseObserver, request.getSequenceId(), queryId));
          } catch (Exception ex) {
            logger.error("Failed to handle result for queryId {}", queryId, ex);
            responseObserver.onError(ex);
          }
        }
      }

      @Override
      public void onError(Throwable t) {
        logger.error("JobResultsService stream failed with error ", t);
        forwarder.get().resultsError(queryId, t);
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
        forwarder.get().resultsCompleted(queryId);
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(allocator);
  }

  private static class JobResultsGrpcLocalResponseSender implements ResponseSender {
    private StreamObserver<JobResultsResponse> responseStreamObserver;
    private long sequenceId;
    private String queryId;
    private AtomicBoolean sentFailure = new AtomicBoolean(false);

    JobResultsGrpcLocalResponseSender(StreamObserver<JobResultsResponse> responseStreamObserver, long sequenceId,
                                      String queryId) {
      this.responseStreamObserver = responseStreamObserver;
      this.sequenceId = sequenceId;
      this.queryId = queryId;
    }

    @Override
    public void send(Response r) {
      JobResultsResponse.Builder builder = JobResultsResponse.newBuilder();
      builder.setAck(Acks.OK).setSequenceId(sequenceId);
      responseStreamObserver.onNext(builder.build());
    }

    @Override
    public void sendFailure(UserRpcException e) {
      if (sentFailure.compareAndSet(false, true)) {
        logger.error("JobResultsService stream failed with error from result forwarder for queryId {}", queryId, e);
        responseStreamObserver.onError(e);
      }
    }
  }
}
