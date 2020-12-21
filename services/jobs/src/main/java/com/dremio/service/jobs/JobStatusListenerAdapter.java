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

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import com.dremio.common.exceptions.GrpcExceptionUtil;
import com.dremio.common.exceptions.UserException;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.proto.JobId;
import com.google.common.util.concurrent.SettableFuture;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

/**
 * Adapts {@link JobStatusListener} to {@link StreamObserver}.
 */
class JobStatusListenerAdapter implements StreamObserver<JobEvent> {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobStatusListenerAdapter.class);

  private final SettableFuture<JobId> jobId = SettableFuture.create();

  // an exception that occurs before this event is considered a submission failure, otherwise a job failure
  private final AtomicBoolean jobSubmitted = new AtomicBoolean(false);

  private final JobStatusListener statusListener;

  JobStatusListenerAdapter(JobStatusListener statusListener) {
    this.statusListener = statusListener;
  }

  @Override
  public void onNext(JobEvent value) {
    switch (value.getEventCase()) {

    case JOB_ID:
      jobId.set(JobsProtoUtil.toStuff(value.getJobId()));
      break;

    case JOB_SUBMITTED:
      if (jobSubmitted.compareAndSet(false, true)) {
        statusListener.jobSubmitted();
      }
      break;

    case QUERY_METADATA:
      statusListener.metadataCollected(value.getQueryMetadata());
      break;

    case PROGRESS_JOB_SUMMARY:
      break;

    case FINAL_JOB_SUMMARY: {
      final JobSummary finalSummary = value.getFinalJobSummary();
      switch (finalSummary.getJobState()) {
      case CANCELED:
        statusListener.jobCancelled(finalSummary.getCancellationInfo().getMessage());
        break;
      case COMPLETED:
        statusListener.jobCompleted();
        break;
      default:
        logger.error("Unrecognized final job state: {}", finalSummary.getJobState());
        break;
      }
      break;
    }

    default:
      logger.error("Unrecognized event: {}", value.getEventCase());
      break;
    }
  }

  @Override
  public void onError(Throwable t) {
    invokeFailureCallback(jobSubmitted.compareAndSet(false, true)
        ? statusListener::submissionFailed : statusListener::jobFailed, t);
  }

  private void invokeFailureCallback(Consumer<RuntimeException> failureCallback, Throwable t) {
    if (t instanceof StatusRuntimeException) {
      final StatusRuntimeException sre = (StatusRuntimeException) t;
      final Optional<UserException> userException =
          GrpcExceptionUtil.fromStatusRuntimeException(sre);
      if (userException.isPresent()) {
        failureCallback.accept(userException.get());
      } else {
        failureCallback.accept(sre);
      }
    } else if (t instanceof RuntimeException) {
      failureCallback.accept((RuntimeException) t);
    } else {
      failureCallback.accept(new RuntimeException(t));
    }
  }

  @Override
  public void onCompleted() {
    // ignore; all events are delivered through #onNext
  }

  public JobId getJobId() {
    try {
      return jobId.get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
