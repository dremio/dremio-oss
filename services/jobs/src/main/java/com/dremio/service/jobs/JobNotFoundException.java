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

import javax.annotation.Nullable;

import com.dremio.service.job.proto.JobId;

/**
 * Thrown when a job is not found.
 */
public class JobNotFoundException extends JobException {
  private static final long serialVersionUID = 1L;

  public enum CauseOfFailure {
    NOT_FOUND,
    CANCEL_FAILED;

    private String buildErrorMessage(JobId jobId) {
      if (this == CauseOfFailure.CANCEL_FAILED) {
        return "Job " + jobId.getId() + " may have completed and cannot be canceled.";
      }
      return "Missing job " + jobId.getId();
    }
  }

  private final CauseOfFailure errorType;

  public JobNotFoundException(JobId jobId) {
    this(jobId, CauseOfFailure.NOT_FOUND);
  }

  public JobNotFoundException(JobId jobId, CauseOfFailure errorType) {
    this(jobId, errorType, null);
  }

  public JobNotFoundException(JobId jobId, Throwable cause) {
    this(jobId, CauseOfFailure.NOT_FOUND, cause);
  }

  public JobNotFoundException(JobId jobId, CauseOfFailure errorType, Throwable cause) {
    this(jobId, errorType, cause, null);
  }

  public JobNotFoundException(JobId jobId, String error) {
    this(jobId, CauseOfFailure.NOT_FOUND, null, error);
  }

  private JobNotFoundException(JobId jobId, CauseOfFailure errorType, @Nullable Throwable cause, @Nullable String message) {
    super(jobId, message != null ? message : errorType.buildErrorMessage(jobId), cause);
    this.errorType = errorType;
  }

  public CauseOfFailure getErrorType() {
    return errorType;
  }
}
