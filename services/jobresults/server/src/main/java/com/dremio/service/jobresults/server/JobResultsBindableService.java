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

import static com.dremio.services.jobresults.common.JobResultsRequestUtils.getJobResultsMethod;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.service.grpc.CloseableBindableService;
import com.dremio.service.jobresults.JobResultsResponse;
import com.dremio.service.jobresults.JobResultsServiceGrpc;
import com.dremio.services.jobresults.common.JobResultsRequestWrapper;

import io.grpc.ServerServiceDefinition;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;

/**
 * Overrides the default service with new method definitions that allow
 * us to customize request/response marshalling.
 */
public class JobResultsBindableService implements CloseableBindableService {
  private final BufferAllocator allocator;
  private final JobResultsGrpcServerFacade jobResultsGrpcServerFacade;

  public JobResultsBindableService(BufferAllocator allocator, JobResultsGrpcServerFacade jobResultsGrpcServerFacade) {
    this.allocator = allocator;
    this.jobResultsGrpcServerFacade = jobResultsGrpcServerFacade;
  }

  @Override
  public ServerServiceDefinition bindService() {
    ServerServiceDefinition.Builder serviceBuilder = ServerServiceDefinition.builder(JobResultsServiceGrpc.SERVICE_NAME);

    serviceBuilder.addMethod(getJobResultsMethod(allocator),
                             ServerCalls.asyncBidiStreamingCall(new JobResultsMethod()));

    return serviceBuilder.build();
  }

  @Override
  public void close() throws Exception {
  }

  public class JobResultsMethod implements ServerCalls.BidiStreamingMethod<JobResultsRequestWrapper, JobResultsResponse> {

    @Override
    public StreamObserver<JobResultsRequestWrapper> invoke(StreamObserver<JobResultsResponse> responseObserver) {
      return jobResultsGrpcServerFacade.getRequestWrapperStreamObserver(responseObserver);
    }
  }
}
