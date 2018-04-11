/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
 * Job exception class for warning (non-fatal) messages
 */
public class JobWarningException extends JobException {
  private static final long serialVersionUID = -1989717288896970795L;

  public JobWarningException() {
    super();
  }

  public JobWarningException(JobId jobId, String message, Throwable cause) {
    super(jobId, message, cause);
  }

  public JobWarningException(JobId jobId, String message) {
    super(jobId, message);
  }

  public JobWarningException(JobId jobId, Throwable cause) {
    super(jobId, cause);
  }
}
