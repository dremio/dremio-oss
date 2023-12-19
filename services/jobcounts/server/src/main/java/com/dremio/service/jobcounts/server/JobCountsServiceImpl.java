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
package com.dremio.service.jobcounts.server;

import java.util.List;

import com.dremio.common.AutoCloseables;
import com.dremio.service.jobcounts.DeleteJobCountsRequest;
import com.dremio.service.jobcounts.GetJobCountsRequest;
import com.dremio.service.jobcounts.JobCounts;
import com.dremio.service.jobcounts.JobCountsServiceGrpc;
import com.dremio.service.jobcounts.UpdateJobCountsRequest;
import com.dremio.service.jobcounts.server.store.JobCountStore;
import com.google.inject.Inject;
import com.google.protobuf.Empty;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

/**
 * Implementation of gRPC service for Job Counts.
 */
public class JobCountsServiceImpl extends JobCountsServiceGrpc.JobCountsServiceImplBase implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobCountsServiceImpl.class);

  private final JobCountStore jobCountStore;

  @Inject
  public JobCountsServiceImpl(JobCountStore jobCountStore) {
    this.jobCountStore = jobCountStore;
  }

  @Override
  public void getJobCounts(GetJobCountsRequest request, StreamObserver<JobCounts> responseObserver) {
    logger.debug("Got get job counts request {}", request);
    try {
      List<Integer> counts = jobCountStore.getCounts(request.getIdsList(), request.getType(), request.getJobCountsAgeInDays());
      JobCounts.Builder jobCounts = JobCounts.newBuilder();
      jobCounts.addAllCount(counts);
      responseObserver.onNext(jobCounts.build());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Exception getting job counts: ", ex);
      responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void updateJobCounts(UpdateJobCountsRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("Got update job counts request {}", request);
    try {
      jobCountStore.bulkUpdateCount(request.getCountUpdatesList());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Exception in update job counts: ", ex);
      responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void deleteJobCounts(DeleteJobCountsRequest request, StreamObserver<Empty> responseObserver) {
    logger.debug("Got delete job counts request: {}", request);
    try {
      jobCountStore.bulkDeleteCount(request.getIdsList());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    } catch (Exception ex) {
      logger.error("Exception in delete job counts: ", ex);
      responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage()).asRuntimeException());
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(jobCountStore);
  }
}
