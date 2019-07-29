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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.job.JobResourcePath;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.JobNotFoundException;

/**
 * Thrown when a job is not found.
 */
public class JobResourceNotFoundException extends NotFoundException {
  private static final long serialVersionUID = 1L;

  private final JobId jobId;

  public JobResourceNotFoundException(JobId jobId, String error) {
    super(new JobResourcePath(jobId), error);
    this.jobId = jobId;
  }

  public JobResourceNotFoundException(JobId jobId, Exception error) {
    super(new JobResourcePath(jobId), "job " + jobId, error);
    this.jobId = jobId;
  }

  public JobResourceNotFoundException(JobId jobId) {
    super(new JobResourcePath(jobId), "Missing job " + jobId);
    this.jobId = jobId;
  }

  private JobResourceNotFoundException(JobId jobId, String error, Throwable cause) {
    super(new JobResourcePath(jobId), error, cause);
    this.jobId = jobId;
  }

  public JobId getJobId() {
    return jobId;
  }

  public static JobResourceNotFoundException fromJobNotFoundException(JobNotFoundException e) {
    return new JobResourceNotFoundException(e.getJobId(), e.getMessage(), e.getCause());
  }
}
