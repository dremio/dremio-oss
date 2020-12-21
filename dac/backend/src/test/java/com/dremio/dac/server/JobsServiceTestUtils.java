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
package com.dremio.dac.server;


import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.CancelJobRequest;
import com.dremio.service.job.DownloadSettings;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.MaterializationSettings;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.CompletionListener;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobSubmittedListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.LocalJobsService;
import com.dremio.service.jobs.MultiJobStatusListener;
import com.dremio.service.jobs.PlanTransformationListener;
import com.dremio.service.jobs.SqlQuery;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Helper methods around {@link com.dremio.service.jobs.JobsService} for testing purposes
 */
public final class JobsServiceTestUtils {

  private JobsServiceTestUtils() {
  }

  /**
   * Submits a Job and returns a {@link JobDataFragment} for a given range of data
   */
  public static JobDataFragment submitJobAndGetData(JobsService jobsService, JobRequest request, int offset,
                                                           int limit, BufferAllocator allocator) {
    final JobId jobId = submitJobAndWaitUntilCompletion(jobsService, request);
    return JobDataWrapper.getJobData(jobsService, allocator, jobId, offset, limit);
  }

  static JobId submitAndWaitUntilSubmitted(JobsService jobsService, JobRequest request) {
    return submitAndWaitUntilSubmitted(jobsService, request, JobStatusListener.NO_OP);
  }

  static JobId submitAndWaitUntilSubmitted(JobsService jobsService, JobRequest request, JobStatusListener listener) {
    final JobSubmittedListener submitListener = new JobSubmittedListener();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), new MultiJobStatusListener(submitListener, listener));
    submitListener.await();
    return jobId;
  }
  public static JobId submitJobAndWaitUntilCompletion(JobsService jobsService, JobRequest request) {
    return submitJobAndWaitUntilCompletion(jobsService, request, JobStatusListener.NO_OP);
  }

  public static JobId submitJobAndWaitUntilCompletion(JobsService jobsService, JobRequest request, JobStatusListener listener) {
    final CompletionListener completionListener = new CompletionListener();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), new MultiJobStatusListener(completionListener, listener));
    completionListener.awaitUnchecked(); // this will throw if the job fails
    if (!completionListener.isCompleted()) {
      throw new RuntimeException("Job has been cancelled");
    }
    return jobId;
  }

  /**
   * Submits query and waits specified milliseconds for completion
   * returns true if query completes in time else cancels job and returns false
   * @param jobsService
   * @param request
   * @param timeOut
   * @throws Exception
   */
  public static boolean submitJobAndCancelOnTimeout(JobsService jobsService, JobRequest request, long timeOut) throws Exception {
    final CompletionListener completionListener = new CompletionListener();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), completionListener);
    completionListener.await(timeOut, TimeUnit.MILLISECONDS);
    if (!completionListener.isCompleted()) {
      jobsService.cancel(CancelJobRequest.newBuilder()
              .setUsername(SYSTEM_USERNAME)
              .setJobId(JobsProtoUtil.toBuf(jobId))
              .setReason("Query did not finish in " + timeOut + "ms")
              .build());
      return false;
    }
    return true;
  }

  /**
   * Submit to the server directly and listen.
   */
  public static JobId submitJobAndWaitUntilCompletion(
      LocalJobsService jobsService,
      JobRequest request,
      PlanTransformationListener planTransformationListener
  ) {
    final CompletionObserver observer = new CompletionObserver();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), observer, planTransformationListener);
    observer.awaitUnchecked(); // this will throw if the job fails
    if (!observer.isCompleted()) {
      throw new RuntimeException("Job has been cancelled");
    }
    return jobId;
  }

  /**
   * Observes for completion.
   */
  private static class CompletionObserver implements StreamObserver<JobEvent> {
    private final CountDownLatch latch = new CountDownLatch(1);

    private volatile Throwable ex;
    private volatile boolean completed = false;

    @Override
    public void onNext(JobEvent value) {
      if (value.hasFinalJobSummary()) {
        final JobSummary finalSummary = value.getFinalJobSummary();
        if (finalSummary.getJobState() == JobState.COMPLETED) {
          completed = true;
        }
        latch.countDown();
      }
    }

    @Override
    public void onError(Throwable t) {
      if (t instanceof StatusRuntimeException) {
        final Optional<UserException> ue = GrpcExceptionUtil.fromStatusRuntimeException((StatusRuntimeException) t);
        if (ue.isPresent()) {
          ex = ue.get();
        } else {
          ex = t;
        }
      } else {
        ex = t;
      }
      latch.countDown();
    }

    @Override
    public void onCompleted() {
    }

    boolean isCompleted() {
      return completed;
    }

    void awaitUnchecked() {
      try {
        latch.await();
      } catch (Exception e) {
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }

      if (ex != null) {
        Throwables.throwIfUnchecked(ex);
        throw new RuntimeException(ex);
      }
    }
  }

  static UserBitShared.QueryProfile getQueryProfile(JobsService jobsService, JobRequest request) throws Exception {
    final JobId jobId = submitJobAndWaitUntilCompletion(jobsService, request);
    return jobsService.getProfile(
      QueryProfileRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setUserName(request.getSqlQuery().getUsername())
        .setAttempt(0)
        .build());
  }

  public static void setSystemOption(JobsService jobsService, String optionName, String optionValue) {
    final String query = String.format("ALTER SYSTEM SET \"%s\"=%s", optionName, optionValue);
    submitJobAndWaitUntilCompletion(jobsService,
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  static void resetSystemOption(JobsService jobsService, String optionName) {
    final String query = String.format("ALTER SYSTEM RESET \"%s\"", optionName);
    submitJobAndWaitUntilCompletion(jobsService,
      JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(query, SampleDataPopulator.DEFAULT_USER_NAME))
        .setQueryType(QueryType.UI_INTERNAL_RUN)
        .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
        .build());
  }

  /**
   * Converts jobRequest to SubmitJobRequest to avoid refactoring in tests
   */
  public static SubmitJobRequest toSubmitJobRequest(JobRequest jobRequest) {
    final SubmitJobRequest.Builder jobRequestBuilder = SubmitJobRequest.newBuilder()
      .setSqlQuery(JobsProtoUtil.toBuf(jobRequest.getSqlQuery()))
      .setQueryType(JobsProtoUtil.toBuf(jobRequest.getQueryType()))
      .setRunInSameThread(jobRequest.runInSameThread());

    if (jobRequest.getRequestType() == JobRequest.RequestType.DOWNLOAD) {
      final DownloadSettings.Builder downloadSettingsBuilder = DownloadSettings.newBuilder();
      if (!Strings.isNullOrEmpty(jobRequest.getDownloadId()))  {
        downloadSettingsBuilder.setDownloadId(jobRequest.getDownloadId());
      }
      if (!Strings.isNullOrEmpty(jobRequest.getFileName())) {
        downloadSettingsBuilder.setFilename(jobRequest.getFileName());
      }
      downloadSettingsBuilder.setRunInSingleThread(jobRequest.runInSingleThread());
      jobRequestBuilder.setDownloadSettings(downloadSettingsBuilder);
    } else if (jobRequest.getRequestType() == JobRequest.RequestType.MATERIALIZATION) {
      final MaterializationSettings.Builder materializationSettingsBuilder = MaterializationSettings.newBuilder();
      if (jobRequest.getMaterializationSummary() != null) {
        materializationSettingsBuilder.setMaterializationSummary(JobsProtoUtil.toBuf(jobRequest.getMaterializationSummary()));
      }
      if (jobRequest.getSubstitutionSettings() != null) {
        materializationSettingsBuilder.setSubstitutionSettings(JobsProtoUtil.toBuf(jobRequest.getSubstitutionSettings()));
      }
      jobRequestBuilder.setMaterializationSettings(materializationSettingsBuilder);
    }

    if (!Strings.isNullOrEmpty(jobRequest.getUsername())) {
      jobRequestBuilder.setUsername(jobRequest.getUsername());
    }

    final VersionedDatasetPath.Builder versionedDatasetPathBuilder = VersionedDatasetPath.newBuilder();
    if (jobRequest.getDatasetPathComponents() != null) {
      versionedDatasetPathBuilder.addAllPath(jobRequest.getDatasetPathComponents());
    }

    if (!Strings.isNullOrEmpty(jobRequest.getDatasetVersion())) {
      versionedDatasetPathBuilder.setVersion(jobRequest.getDatasetVersion());
    }

    jobRequestBuilder.setVersionedDataset(versionedDatasetPathBuilder);
    return jobRequestBuilder.build();
  }
}
