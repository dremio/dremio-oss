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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.ChronicleGrpc.ChronicleBlockingStub;
import com.dremio.service.job.JobCounts;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobStats;
import com.dremio.service.job.JobStatsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceBlockingStub;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;

import io.grpc.StatusRuntimeException;

/**
 * This is used by the clients of {@link JobsService}. This service redirects calls to {@link LocalJobsService} over
 * gRPC if the API is implemented.
 */
@Deprecated //TODO DX-19547: Remove HJS
public class HybridJobsService implements JobsService {
  private static final Logger logger = LoggerFactory.getLogger(HybridJobsService.class);

  private final GrpcChannelBuilderFactory grpcFactory;
  private final Provider<BufferAllocator> allocator;
  private Provider<Integer> portProvider = null;

  private volatile JobsClient jobsClient = null;

  public HybridJobsService(
    GrpcChannelBuilderFactory grpcFactory,
    Provider<BufferAllocator> allocator
  ) {
    this.grpcFactory = grpcFactory;
    this.allocator = allocator;
  }

  @Override
  public void start() {
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(jobsClient);
  }

  @Override
  public JobsClient getJobsClient() {
    // lazily initialized as the endpoint information might not be available in the service set
    if (jobsClient == null) {
      synchronized (this) {
        if (jobsClient == null) {
          // we use a local variable to ensure all blocked threads on this block will only be able to
          // use jobsClient after start() is done
          final JobsClient client = new JobsClient(grpcFactory, allocator, portProvider);
          client.start();
          jobsClient = client;
        }
      }
    }
    return jobsClient;
  }

  public void setPortProvider(Provider<Integer> portProvider) {
    this.portProvider = portProvider;
  }


  private JobsServiceBlockingStub getBlockingStub() {
    return getJobsClient().getBlockingStub();
  }

  private JobsServiceStub getAsyncStub() {
    return getJobsClient().getAsyncStub();
  }

  private ChronicleBlockingStub getChronicleBlockingStub() {
    return getJobsClient().getChronicleBlockingStub();
  }

  @Override
  public JobId submitJob(SubmitJobRequest jobRequest, JobStatusListener statusListener) {
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(statusListener);
    getAsyncStub().submitJob(jobRequest, adapter);
    return adapter.getJobId();
  }

  @Override
  public JobDetails getJobDetails(JobDetailsRequest request) throws JobNotFoundException {
    try {
      return getChronicleBlockingStub().getJobDetails(request);
    } catch (StatusRuntimeException e) {
      throwSuitableException(e, JobsProtoUtil.toStuff(request.getJobId()), request.getUserName());
      throw new AssertionError(e); // should be unreachable
    }
  }

  @Override
  public JobSummary getJobSummary(JobSummaryRequest request) throws JobNotFoundException {
    try {
      return getChronicleBlockingStub().getJobSummary(request);
    } catch (StatusRuntimeException e) {
      throwSuitableException(e, JobsProtoUtil.toStuff(request.getJobId()), request.getUserName());
      throw new AssertionError(e); // should be unreachable
    }
  }

  @Override
  public JobCounts getJobCounts(JobCountsRequest request) {
    try {
      return getChronicleBlockingStub().getJobCounts(request);
    } catch (StatusRuntimeException e) {
      throwIfUserException(e);
      throw e;
    }
  }

  @Override
  public JobStats getJobStats(JobStatsRequest request) {
    try {
      return getChronicleBlockingStub().getJobStats(request);
    } catch (StatusRuntimeException e) {
      throwIfUserException(e);
      throw e;
    }
  }

  @Override
  public Iterable<JobSummary> searchJobs(SearchJobsRequest request) {
    final List<JobSummary> jobSummaries = new ArrayList<>();
    getChronicleBlockingStub().searchJobs(request).forEachRemaining(jobSummaries::add);
    return jobSummaries;
  }

  @Override
  public Iterable<JobDetails> getJobsForParent(JobsWithParentDatasetRequest jobsWithParentDatasetRequest) {
    final List<JobDetails> jobDetails = new ArrayList<>();
    getChronicleBlockingStub().getJobsForParent(jobsWithParentDatasetRequest).forEachRemaining(jobDetails::add);
    return jobDetails;
  }

  @Override
  public QueryProfile getProfile(QueryProfileRequest request) throws JobNotFoundException {
    try {
      return getChronicleBlockingStub().getProfile(request);
    } catch (StatusRuntimeException e) {
      // TODO (DX-17909): Use request username
      throwSuitableException(e, JobsProtoUtil.toStuff(request.getJobId()), SYSTEM_USERNAME);
      throw new AssertionError(e); // should be unreachable
    }
  }

  @Override
  public void cancel(CancelJobRequest request) throws JobException {
    try {
      getBlockingStub().cancel(request);
    } catch (StatusRuntimeException e) {
      throwSuitableException(e, JobsProtoUtil.toStuff(request.getJobId()), request.getUsername());
      throw new AssertionError(e); // should be unreachable
    }
  }

  @Override
  public void registerListener(JobId jobId, ExternalStatusListener listener) {
    ExternalListenerAdapter adapter = new ExternalListenerAdapter(listener);
    getAsyncStub().subscribeToJobEvents(JobsProtoUtil.toBuf(jobId), adapter);
  }

  private static void throwSuitableException(StatusRuntimeException sre, JobId jobId, String username)
      throws JobNotFoundException {
    throwIfUserException(sre);

    switch (sre.getStatus().getCode()) {
    case NOT_FOUND:
      throw new JobNotFoundException(jobId, sre);
    case PERMISSION_DENIED:
      throw new AccessControlException(
          String.format("User [%s] does not have permission to access job [%s]", username, jobId));
    default:
      throw sre;
    }
  }

  private static void throwIfUserException(StatusRuntimeException e) {
    final Optional<UserException> userException = JobsRpcUtils.fromStatusRuntimeException(e);
    if (userException.isPresent()) {
      throw userException.get();
    }
  }
}
