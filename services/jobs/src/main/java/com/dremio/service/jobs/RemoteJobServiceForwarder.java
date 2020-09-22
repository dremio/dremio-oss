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
package com.dremio.service.jobs;

import java.util.concurrent.ExecutionException;

import javax.inject.Provider;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;


/**
 * forwards the request to target jobserver
 */
public class RemoteJobServiceForwarder {
  private final Provider<ConduitProvider> conduitProvider;

  public RemoteJobServiceForwarder(final Provider<ConduitProvider> conduitProvider) {
    this.conduitProvider = conduitProvider;
  }

  public void subscribeToJobEvents(CoordinationProtos.NodeEndpoint target,
                                   JobProtobuf.JobId request,
                                   StreamObserver<JobEvent> responseObserver) throws ExecutionException {
    final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(target);
    final JobsServiceGrpc.JobsServiceStub asyncStub = JobsServiceGrpc.newStub(channel);
    asyncStub.subscribeToJobEvents(request, new RemoteResponseObserverWrapper<JobEvent>(responseObserver));
  }

  public JobDetails getJobDetails(CoordinationProtos.NodeEndpoint target, JobDetailsRequest request) throws ExecutionException {
    final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(target);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return stub.getJobDetails(request);
  }

  public JobSummary getJobSummary(CoordinationProtos.NodeEndpoint target, JobSummaryRequest request) throws ExecutionException {
    final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(target);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return stub.getJobSummary(request);
  }

  public UserBitShared.QueryProfile getProfile(CoordinationProtos.NodeEndpoint target, QueryProfileRequest queryProfileRequest) {
    final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(target);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return stub.getProfile(queryProfileRequest);
  }

  /**
   * Relays back the response from remote server back to the caller
   * observer.
   * @param <T>
   */
  private class RemoteResponseObserverWrapper<T> implements StreamObserver<T> {
    private StreamObserver<T> underlyingObserver;

    public RemoteResponseObserverWrapper(StreamObserver<T> in) {
      this.underlyingObserver = in;
    }

    @Override
    public void onNext(T value) {
      this.underlyingObserver.onNext(value);
    }

    @Override
    public void onError(Throwable t) {
      this.underlyingObserver.onError(t);
    }

    @Override
    public void onCompleted() {
      this.underlyingObserver.onCompleted();
    }
  }

}
