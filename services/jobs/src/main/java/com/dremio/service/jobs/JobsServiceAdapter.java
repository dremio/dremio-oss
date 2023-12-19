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

import javax.inject.Provider;

import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.CancelReflectionJobRequest;
import com.dremio.service.job.DeleteJobCountsRequest;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.ReflectionJobEventsRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobProtobuf;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

/**
 * Adapts {@link LocalJobsService} to {@link com.dremio.service.job.JobsServiceGrpc.JobsServiceImplBase}.
 */
@VisibleForTesting
public class JobsServiceAdapter extends JobsServiceGrpc.JobsServiceImplBase {
  private final Provider<LocalJobsService> localJobsServiceProvider;

  public JobsServiceAdapter(final Provider<LocalJobsService> jobsService) {
    this.localJobsServiceProvider = jobsService;
  }

  private LocalJobsService getJobsService() {
    return this.localJobsServiceProvider.get();
  }

  @Override
  public void subscribeToJobEvents(JobProtobuf.JobId request, StreamObserver<JobEvent> responseObserver) {
    getJobsService().registerListener(JobsProtoUtil.toStuff(request), responseObserver);
  }

  @Override
  public void submitJob(SubmitJobRequest request, StreamObserver<JobEvent> responseObserver) {
    getJobsService().submitJob(request, responseObserver);
  }

  @Override
  public void cancel(CancelJobRequest request, StreamObserver<Empty> responseObserver) {
    try {
      getJobsService().cancel(request);
    } catch (Exception e) {
      JobsRpcUtils.handleException(responseObserver, e);
      return;
    }

    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void subscribeToReflectionJobEvents(ReflectionJobEventsRequest request,
                                             StreamObserver<JobEvent> responseObserver) {
    getJobsService().registerReflectionJobListener(request, responseObserver);
  }

  @Override
  public void cancelReflectionJob(CancelReflectionJobRequest request, StreamObserver<Empty> responseObserver) {
    try {
      getJobsService().cancelReflectionJob(request);
    } catch (Exception e) {
      JobsRpcUtils.handleException(responseObserver, e);
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void deleteJobCounts(DeleteJobCountsRequest request, StreamObserver<Empty> responseObserver) {
    try {
      getJobsService().deleteJobCounts(request);
    } catch (Exception e) {
      JobsRpcUtils.handleException(responseObserver, e);
    }
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }
}
