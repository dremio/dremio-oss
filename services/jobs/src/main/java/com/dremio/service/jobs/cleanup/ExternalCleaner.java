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
package com.dremio.service.jobs.cleanup;

import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.service.job.proto.JobAttempt;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Define how external dependencies will be deleted */
public abstract class ExternalCleaner {

  private static final int MAX_REC_ERROR = 10;
  private final Map<JobAttempt, Throwable> failedAttempts = new HashMap<>();
  private long successCounter = 0;
  private long failedCounter = 0;

  /**
   * Cleanup logic performed by the cleaner implementation.
   *
   * @param jobAttempt job attempt information.
   */
  protected abstract void doGo(JobAttempt jobAttempt);

  /**
   * Returns the External Cleaner name.
   *
   * @return the name of the cleaner.
   */
  public String getName() {
    return this.getClass().getSimpleName();
  }

  /**
   * Delete an external dependency by attempt.
   *
   * @param jobAttempt job attempt associated
   */
  final boolean go(JobAttempt jobAttempt) {
    try {
      doGo(jobAttempt);
      successCounter++;
      return true;
    } catch (Exception e) {
      failedCounter++;
      recordException(jobAttempt, e);
    }
    return false;
  }

  /**
   * Return the number of {@link JobAttempt} cleaned by the external dependency.
   *
   * @return total successes for the cleaner.
   */
  final long getSuccessCounter() {
    return successCounter;
  }

  /**
   * Gets the total number of failures for the execution of the cleaner.
   *
   * @return total failures for the cleaner.
   */
  final long getFailedCounter() {
    return failedCounter;
  }

  final List<String> getLastErrors() {
    return failedAttempts.entrySet().stream()
        .map(
            e ->
                String.format(
                    "JobId: %s, Error: %s",
                    AttemptIdUtils.fromString(e.getKey().getAttemptId()),
                    e.getValue().getMessage()))
        .collect(Collectors.toList());
  }

  private void recordException(JobAttempt attempt, Exception e) {
    if (failedCounter <= MAX_REC_ERROR) {
      failedAttempts.put(attempt, e);
    }
  }
}
