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

import com.dremio.service.job.proto.JobId;

/**
 * Thrown when a job is not found.
 */
public class JobNotFoundException extends JobException {
  private static final long serialVersionUID = 1L;

  public enum causeOfFailure{
    NOT_FOUND,
    CANCEL_FAILED
  }

  private final JobId jobId;
  private causeOfFailure errorType = causeOfFailure.NOT_FOUND ;

  public JobNotFoundException(JobId jobId, String error) {
    super(jobId, error);
    this.jobId = jobId;
  }

  public JobNotFoundException(JobId jobId, causeOfFailure errorType) {
    super(jobId, errorType.equals(causeOfFailure.CANCEL_FAILED)?"Job " + jobId.getId() + " may have completed and cannot be canceled."
      :"Missing job " + jobId.getId());
    this.jobId = jobId;
    this.errorType = errorType;
  }

  public JobNotFoundException(JobId jobId, Exception error) {
    super(jobId, "Missing job " + jobId.getId(), error);
    this.jobId = jobId;
  }

  public JobNotFoundException(JobId jobId) {
    super(jobId, "Missing job " + jobId.getId());
    this.jobId = jobId;
  }

  @Override
  public JobId getJobId() {
    return jobId;
  }

  public causeOfFailure getErrorType(){return errorType;}
}
