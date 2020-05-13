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

import static com.dremio.service.jobs.JobsRpcUtils.getJobsPort;

import java.io.IOException;

import javax.inject.Provider;

import org.apache.arrow.flight.FlightGrpcUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.service.maestro.MaestroGrpcServerFacade;
import com.dremio.sabot.rpc.ExecToCoordResultsHandler;
import com.dremio.sabot.rpc.ExecToCoordStatusHandler;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.StoreJobResultRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobProtobuf.JobId;
import com.dremio.service.jobresults.server.JobResultsGrpcServerFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.protobuf.Empty;

import io.grpc.Server;
import io.grpc.stub.StreamObserver;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;

/**
 * Server that maintains the lifecycle of a gRPC server that handles jobs RPC requests by invoking the
 * {@link LocalJobsService} in this JVM instance.
 */
public class JobsServer implements Service {
  private static final Logger logger = LoggerFactory.getLogger(JobsServer.class);

  private final Provider<LocalJobsService> jobsService;
  private final GrpcServerBuilderFactory grpcFactory;
  private final BufferAllocator allocator;
  private final BufferAllocator jobResultsAllocator;
  private final Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider;
  private final Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider;

  private Server server = null;
  private JobsFlightProducer producer = null;

  public JobsServer(
      Provider<LocalJobsService> jobsService,
      GrpcServerBuilderFactory grpcFactory,
      Provider<ExecToCoordStatusHandler> execToCoordStatusHandlerProvider,
      Provider<ExecToCoordResultsHandler> execToCoordResultsHandlerProvider,
      BufferAllocator allocator
  ) {
    this.jobsService = jobsService;
    this.grpcFactory = grpcFactory;
    this.allocator = allocator.newChildAllocator(getClass().getName(), 0, Long.MAX_VALUE);
    this.jobResultsAllocator = allocator.newChildAllocator("JobResultsGrpcServer", 0, Long.MAX_VALUE);
    this.execToCoordStatusHandlerProvider = execToCoordStatusHandlerProvider;
    this.execToCoordResultsHandlerProvider = execToCoordResultsHandlerProvider;
  }

  @Override
  public void start() throws IOException {
    producer = new JobsFlightProducer(jobsService.get(), allocator);
    server = JobsRpcUtils.newServerBuilder(grpcFactory, getJobsPort())
        .maxInboundMetadataSize(Integer.MAX_VALUE) // Accomodate large error messages like OOM
        // all clients of jobs service are internal services, so sending sensitive server-side state is safe
        .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
        .addService(new JobsServiceAdapter())
        .addService(new Chronicle())
        .addService(FlightGrpcUtils.createFlightService(allocator, producer, null, null))
        .addService(new MaestroGrpcServerFacade(execToCoordStatusHandlerProvider))
        .addService(new JobResultsGrpcServerFacade(execToCoordResultsHandlerProvider, jobResultsAllocator))
        .build();

    server.start();

    logger.info("JobsServer is up. Listening on port '{}'", server.getPort());
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(producer, allocator, jobResultsAllocator);
    if (server != null) {
      server.shutdown();
    }
  }

  private LocalJobsService getJobsService() {
    return jobsService.get();
  }

  public int getPort() {
    Preconditions.checkNotNull(server, "JobsServer was not started (see #start)");
    return server.getPort();
  }

  /**
   * Adapts {@link LocalJobsService} to {@link com.dremio.service.job.JobsServiceGrpc.JobsServiceImplBase}.
   */
  @VisibleForTesting
  class JobsServiceAdapter extends JobsServiceGrpc.JobsServiceImplBase {

    @Override
    public void subscribeToJobEvents(JobId request, StreamObserver<JobEvent> responseObserver) {
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
  }

  /**
   * Adapts {@link LocalJobsService} point lookup methods to {@link ChronicleGrpc.ChronicleImplBase}
   */
  @VisibleForTesting
  class Chronicle extends ChronicleGrpc.ChronicleImplBase {

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
    public void getProfile(QueryProfileRequest request, StreamObserver<QueryProfile> responseObserver) {
      handleUnaryCall(getJobsService()::getProfile, request, responseObserver);
    }

    @Override
    public void searchJobs(SearchJobsRequest request, StreamObserver<JobSummary> responseObserver) {
      getJobsService().searchJobs(request).forEach(responseObserver::onNext);
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
