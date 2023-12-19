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

import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.util.Retryer;
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
import com.dremio.service.job.NodeStatusRequest;
import com.dremio.service.job.NodeStatusResponse;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.google.common.collect.ImmutableSet;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;


/**
 * forwards the request to target jobserver
 */
public class RemoteJobServiceForwarder {
  private static final int MAX_RETRIES = 3;
  private static final Set<Status.Code> retriableGrpcStatuses =
    ImmutableSet.of(Status.UNAVAILABLE.getCode(), Status.DEADLINE_EXCEEDED.getCode());
  private final Provider<ConduitProvider> conduitProvider;
  private final Retryer retryer;

  public RemoteJobServiceForwarder(final Provider<ConduitProvider> conduitProvider) {
    this.conduitProvider = conduitProvider;
    this.retryer = Retryer.newBuilder()
      .retryOnExceptionFunc(this::isRetriableException)
      .setMaxRetries(MAX_RETRIES)
      .build();
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

  public NodeStatusResponse getNodeStatus(CoordinationProtos.NodeEndpoint target, NodeStatusRequest request) {
    final ManagedChannel channel = conduitProvider.get().getOrCreateChannel(target);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return retryer.call(() -> stub.withDeadlineAfter(10, TimeUnit.SECONDS).getNodeStatus(request));
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

  private boolean isRetriableException(Throwable e) {
    if (e instanceof StatusRuntimeException) {
      if (retriableGrpcStatuses.contains(((StatusRuntimeException)e).getStatus().getCode())) {
        return true;
      }
    }
    if (e instanceof CompletionException || e instanceof ExecutionException) {
      if (e.getCause() instanceof StatusRuntimeException) {
        return retriableGrpcStatuses.contains(((StatusRuntimeException) e.getCause()).getStatus().getCode());
      }
    }
    return false;
  }
}
