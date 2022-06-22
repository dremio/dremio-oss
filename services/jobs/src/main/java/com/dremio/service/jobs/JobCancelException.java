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

/**
 * Job exception class for warning (non-fatal) messages.
 * This error is thrown when there is a cancel request for an already completed job. It returns 409 CONFLICT HTTP code
 */
public class JobCancelException extends RuntimeException {
  private static final long serialVersionUID = -1989717288896970795L;

  public JobCancelException() {
    super();
  }

  public JobCancelException(String message, Throwable cause) {
    super(message);
  }

  public JobCancelException(String message) {
    super(message);
  }

  public JobCancelException(Throwable cause) {
    super(cause);
  }
}
