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

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
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

  private Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider;
  private final BufferAllocator allocator;

  public JobResultsGrpcServerFacade(Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider,
                                    BufferAllocator allocator) {
    this.execToCoordResultsHandlerProvider = execToCoordResultsHandlerProvider;
    this.allocator = allocator;
  }

  public StreamObserver<JobResultsRequest> jobResults(StreamObserver<JobResultsResponse> responseObserver) {

    return new StreamObserver<JobResultsRequest>() {
      @Override
      public void onNext(JobResultsRequest request) {
        try {
          ByteBuf dBody = NettyArrowBuf.unwrapBuffer(allocator.buffer(request.getData().size()));
          dBody.writeBytes(request.getData().toByteArray());

          execToCoordResultsHandlerProvider.get().dataArrived(request.getHeader(), dBody, request,
                  new JobResultsGrpcLocalResponseSender(responseObserver, request.getSequenceId()));
        } catch (Exception ex) {
          responseObserver.onError(ex);
        }
      }

      @Override
      public void onError(Throwable t) {
        responseObserver.onError(t);
      }

      @Override
      public void onCompleted() {
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

    JobResultsGrpcLocalResponseSender(StreamObserver<JobResultsResponse> responseStreamObserver, long sequenceId) {
      this.responseStreamObserver = responseStreamObserver;
      this.sequenceId = sequenceId;
    }

    @Override
    public void send(Response r) {
      JobResultsResponse.Builder builder = JobResultsResponse.newBuilder();
      builder.setAck(Acks.OK).setSequenceId(sequenceId);
      responseStreamObserver.onNext(builder.build());
    }

    @Override
    public void sendFailure(UserRpcException e) {
    }
  }
}
