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

import java.util.Iterator;
import java.util.concurrent.Executor;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.datastore.EnumSearchValueNotFoundException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.grpc.OnReadyHandler;
import com.dremio.service.job.ActiveJobSummary;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.JobAndUserStats;
import com.dremio.service.job.JobAndUserStatsRequest;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.NodeStatusRequest;
import com.dremio.service.job.NodeStatusResponse;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.RecentJobSummary;
import com.dremio.service.job.ReflectionJobDetailsRequest;
import com.dremio.service.job.ReflectionJobProfileRequest;
import com.dremio.service.job.ReflectionJobSummaryRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SearchReflectionJobsRequest;
import com.dremio.service.job.StoreJobResultRequest;
import com.dremio.service.job.UniqueUserStats;
import com.dremio.service.job.UniqueUserStatsRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.protobuf.Empty;

import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

/**
 * Adapts {@link LocalJobsService} point lookup methods to {@link ChronicleGrpc.ChronicleImplBase}
 */
@VisibleForTesting
public class Chronicle extends ChronicleGrpc.ChronicleImplBase {

  private final Provider<LocalJobsService> localJobsServiceProvider;
  private final Provider<Executor> executor;
  private static final Logger LOGGER = LoggerFactory.getLogger(Chronicle.class);

  public Chronicle(final Provider<LocalJobsService> jobsService, Provider<Executor> executor) {
    this.localJobsServiceProvider = jobsService;
    this.executor = executor;
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
  public void getUniqueUserStats(UniqueUserStatsRequest request, StreamObserver<UniqueUserStats> responseObserver) {
    handleUnaryCall(getJobsService()::getUniqueUserStats, request, responseObserver);
  }

  @Override
  public void getJobAndUserStats(JobAndUserStatsRequest request, StreamObserver<JobAndUserStats> responseObserver) {
    handleUnaryCall(getJobsService()::getJobAndUserStats, request, responseObserver);
  }

  @Override
  public void getProfile(QueryProfileRequest request, StreamObserver<UserBitShared.QueryProfile> responseObserver) {
    handleUnaryCall(getJobsService()::getProfile, request, responseObserver);
  }

  @Override
  public void searchJobs(SearchJobsRequest request, StreamObserver<JobSummary> responseObserver) {
    final ServerCallStreamObserver<JobSummary> streamObserver = (ServerCallStreamObserver<JobSummary>) responseObserver;

    final Iterator<JobSummary> jobs = getJobsService().searchJobs(request).iterator();
    final class SearchJobs extends OnReadyHandler<JobSummary> {
      SearchJobs() {
        super("search-jobs", Chronicle.this.executor.get(), streamObserver, jobs);
      }
    }

    final SearchJobs searchJobs = new SearchJobs();
    streamObserver.setOnReadyHandler(searchJobs);
    streamObserver.setOnCancelHandler(searchJobs::cancel);
  }

  @Override
  public void getActiveJobs(com.dremio.service.job.ActiveJobsRequest request,
                            io.grpc.stub.StreamObserver<ActiveJobSummary> responseObserver) {
    final ServerCallStreamObserver<ActiveJobSummary> streamObserver = (ServerCallStreamObserver<ActiveJobSummary>) responseObserver;
    final Iterator<ActiveJobSummary> wrapperIterator = new Iterator<ActiveJobSummary>() {
      // skip nulls
      private final Iterator<ActiveJobSummary> inner = getJobsService().getActiveJobs(request).iterator();
      private ActiveJobSummary prefetched;
      private boolean isFirst = true;

      @Override
      public boolean hasNext() {
        if (isFirst) {
          prefetch();
          isFirst = false;
        }
        return prefetched != null;
      }

      @Override
      public ActiveJobSummary next() {
        ActiveJobSummary ret = prefetched;
        prefetch();
        return ret;
      }

      private void prefetch() {
        prefetched = null;
        try {
          while (inner.hasNext()) {
            prefetched = inner.next();
            // skip nulls
            if (prefetched != null) {
              return;
            }
          }
        } catch (EnumSearchValueNotFoundException ex) {
          LOGGER.info("Got EnumSearchValueNotFoundException returning empty response, query {}", request.getQuery(), ex);
          // ignore exception, returning here ends the iterator.
        } catch (Exception ex) {
          LOGGER.error("Exception while fetching active jobs for request {}", request, ex);
          throw ex;
        }
      }
    };

    final class ActiveJobs extends OnReadyHandler<ActiveJobSummary> {
      ActiveJobs() {
        super("active-jobs", Chronicle.this.executor.get(), streamObserver, wrapperIterator);
      }
    }

    final ActiveJobs activeJobs = new ActiveJobs();
    streamObserver.setOnReadyHandler(activeJobs);
    streamObserver.setOnCancelHandler(activeJobs::cancel);
  }

  @Override
  public void getRecentJobs(com.dremio.service.job.RecentJobsRequest request,
                            io.grpc.stub.StreamObserver<RecentJobSummary> responseObserver) {
    final ServerCallStreamObserver<RecentJobSummary> streamObserver = (ServerCallStreamObserver<RecentJobSummary>) responseObserver;
    final Iterator<RecentJobSummary> wrapperIterator = new Iterator<RecentJobSummary>() {
      // skip nulls
      private final Iterator<RecentJobSummary> inner = getJobsService().getRecentJobs(request).iterator();
      private RecentJobSummary prefetched;
      private boolean isFirst = true;

      @Override
      public boolean hasNext() {
        if (isFirst) {
          prefetch();
          isFirst = false;
        }
        return prefetched != null;
      }

      @Override
      public RecentJobSummary next() {
        RecentJobSummary ret = prefetched;
        prefetch();
        return ret;
      }

      private void prefetch() {
        prefetched = null;
        try {
          while (inner.hasNext()) {
            prefetched = inner.next();
            // skip nulls
            if (prefetched != null) {
              return;
            }
          }
        } catch (EnumSearchValueNotFoundException ex) {
          LOGGER.info("Got EnumSearchValueNotFoundException returning empty response, query {}", request.getQuery(), ex);
          // ignore exception, returning here ends the iterator.
        } catch (Exception ex) {
          LOGGER.error("Exception while fetching recent jobs for request {}", request, ex);
          throw ex;
        }
      }
    };

    final class RecentJobs extends OnReadyHandler<RecentJobSummary> {
      RecentJobs() {
        super("recent-jobs", Chronicle.this.executor.get(), streamObserver, wrapperIterator);
      }
    }

    final RecentJobs recentJobs = new RecentJobs();
    streamObserver.setOnReadyHandler(recentJobs);
    streamObserver.setOnCancelHandler(recentJobs::cancel);
  }

  @Override
  public void getJobsForParent(JobsWithParentDatasetRequest request, StreamObserver<JobDetails> responseObserver) {
    final ServerCallStreamObserver<JobDetails> streamObserver = (ServerCallStreamObserver<JobDetails>) responseObserver;

    final Iterator<JobDetails> jobs = getJobsService().getJobsForParent(request).iterator();
    final class GetJobsForParent extends OnReadyHandler<JobDetails> {
      GetJobsForParent() {
        super("jobs-for-parent", Chronicle.this.executor.get(), streamObserver, jobs);
      }
    }

    final GetJobsForParent jobsForParent = new GetJobsForParent();
    streamObserver.setOnReadyHandler(jobsForParent);
    streamObserver.setOnCancelHandler(jobsForParent::cancel);
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
    final ServerCallStreamObserver<JobSummary> streamObserver = (ServerCallStreamObserver<JobSummary>) responseObserver;
    final class SearchReflectionJobs extends OnReadyHandler<JobSummary> {
      SearchReflectionJobs() {
        super("search-reflection-jobs", Chronicle.this.executor.get(), streamObserver,
          new ErrorConvertingIterator<>(() -> getJobsService().searchReflectionJobs(request).iterator()));
      }
    }

    final SearchReflectionJobs searchReflectionJobs = new SearchReflectionJobs();
    streamObserver.setOnReadyHandler(searchReflectionJobs);
    streamObserver.setOnCancelHandler(searchReflectionJobs::cancel);
  }

  @Override
  public void getReflectionJobProfile(ReflectionJobProfileRequest request, StreamObserver<UserBitShared.QueryProfile> responseObserver) {
    handleUnaryCall(getJobsService()::getReflectionJobProfile, request, responseObserver);
  }

  @Override
  public void getNodeStatus(NodeStatusRequest request, StreamObserver<NodeStatusResponse> responseObserver) {
    handleUnaryCall(getJobsService()::getNodeStatus, request, responseObserver);
  }

  private static class ErrorConvertingIterator<E> implements Iterator<E> {
    private final Provider<Iterator<E>> iteratorProvider;
    private Iterator<E> iterator;

    ErrorConvertingIterator(Provider<Iterator<E>> iteratorProvider) {
      this.iteratorProvider = iteratorProvider;
    }

    @Override
    public boolean hasNext() {
      try {
        if (iterator == null) {
          iterator = iteratorProvider.get();
        }
        return iterator.hasNext();
      } catch (Throwable ex) {
        Throwable throwable = JobsRpcUtils.convertToGrpcException(ex);
        Throwables.throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
      }
    }

    @Override
    public E next() {
      try {
        return iterator.next();
      } catch (Throwable ex) {
        Throwable throwable = JobsRpcUtils.convertToGrpcException(ex);
        Throwables.throwIfUnchecked(throwable);
        throw new RuntimeException(throwable);
      }
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
