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

import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.ReflectionJobSummaryRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.StoreJobResultRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;

import io.grpc.stub.StreamObserver;

/**
 * Adapts {@link LocalJobsService} point lookup methods to {@link ChronicleGrpc.ChronicleImplBase}
 */
@VisibleForTesting
public class Chronicle extends ChronicleGrpc.ChronicleImplBase {

  private final Provider<LocalJobsService> localJobsServiceProvider;

  public Chronicle(final Provider<LocalJobsService> jobsService) {
    this.localJobsServiceProvider = jobsService;
  }

  private LocalJobsService getJobsService() {
    return this.localJobsServiceProvider.get();
  }

  @Override
  public void getJobDetails(JobDetailsRequest request, StreamObserver<JobDetails> responseObserver) {
    handleUnaryCall(getJobsService()::getJobDetails, request, responseObserver);
  }

  @Override
  public void getJobCounts(JobCountsRequest request, StreamObserver<JobCounts> responseObserver) {
    handleUnaryCall(getJobsService()::getJobCounts, request, responseObserver);
  }

  @Override
  public void getJobStats(JobStatsRequest request, StreamObserver<JobStats> responseObserver) {
    handleUnaryCall(getJobsService()::getJobStats, request, responseObserver);
  }

  @Override
  public void getProfile(QueryProfileRequest request, StreamObserver<UserBitShared.QueryProfile> responseObserver) {
    handleUnaryCall(getJobsService()::getProfile, request, responseObserver);
  }

  @Override
  public void searchJobs(SearchJobsRequest request, StreamObserver<JobSummary> responseObserver) {
    getJobsService().searchJobs(request).forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public void getActiveJobs(com.dremio.service.job.ActiveJobsRequest request,
                            io.grpc.stub.StreamObserver<com.dremio.service.job.ActiveJobSummary> responseObserver) {
    getJobsService().getActiveJobs(request).forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public void getJobsForParent(JobsWithParentDatasetRequest request, StreamObserver<JobDetails> responseObserver) {
    getJobsService().getJobsForParent(request).forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public void getJobSummary(JobSummaryRequest request, StreamObserver<JobSummary> responseObserver) {
    handleUnaryCall(getJobsService()::getJobSummary, request, responseObserver);
  }

  @Override
  public void storeJobResult(StoreJobResultRequest request, StreamObserver<Empty> responseObserver) {
    try {
      getJobsService().recordJobResult(request);
    } catch (Exception e) {
      JobsRpcUtils.handleException(responseObserver, e);
      return;
    }

    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void getReflectionJobSummary(ReflectionJobSummaryRequest request, StreamObserver<JobSummary> responseObserver) {
    handleUnaryCall(getJobsService()::getReflectionJobSummary, request, responseObserver);
  }

  @Override
  public void getReflectionJobDetails(ReflectionJobDetailsRequest request, StreamObserver<JobDetails> responseObserver) {
    handleUnaryCall(getJobsService()::getReflectionJobDetails, request, responseObserver);
  }

  @Override
  public void searchReflectionJobs(SearchReflectionJobsRequest request, StreamObserver<JobSummary> responseObserver) {
    try {
      getJobsService().searchReflectionJobs(request).forEach(responseObserver::onNext);
    } catch (Exception e) {
      JobsRpcUtils.handleException(responseObserver, e);
    }
    responseObserver.onCompleted();
  }

  @Override
  public void getReflectionJobProfile(ReflectionJobProfileRequest request, StreamObserver<UserBitShared.QueryProfile> responseObserver) {
    handleUnaryCall(getJobsService()::getReflectionJobProfile, request, responseObserver);
  }

  /**
   * Handles a unary call synchronously.
   *
   * @param function function that can apply the request
   * @param request  request
   * @param observer response observer
   * @param <T>      request type
   * @param <R>      response type
   * @param <E>      exception type
   */
  private static <T, R, E extends Exception>
  void handleUnaryCall(ThrowingFunction<T, R, E> function, T request, StreamObserver<R> observer) {
    final R r;
    try {
      r = function.apply(request);
    } catch (Exception e) {
      JobsRpcUtils.handleException(observer, e);
      return;
    }

    observer.onNext(r);
    observer.onCompleted();
  }
  /**
   * Function that throws.
   *
   * @param <T> input type
   * @param <R> result type
   * @param <E> exception type
   */
  @FunctionalInterface
  private interface ThrowingFunction<T, R, E extends Throwable> {
    R apply(T t) throws E;
  }
}
