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

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExternalCleanerRunner {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCleanerRunner.class);
  private final List<ExternalCleaner> externalCleaners;
  private long jobAttemptsFailed = 0;
  private long jobAttemptExecution = 0;

  public ExternalCleanerRunner(final List<ExternalCleaner> externalCleaners) {
    this.externalCleaners = externalCleaners;
  }

  public void run(JobResult jobResult) {
    if (jobResult == null
        || jobResult.getAttemptsList() == null
        || jobResult.getAttemptsList().isEmpty()) {
      return;
    }

    for (JobAttempt jobAttempt : jobResult.getAttemptsList()) {
      jobAttemptExecution++;
      for (ExternalCleaner cleaner : externalCleaners) {
        boolean successfulExecution = cleaner.go(jobAttempt);
        if (!successfulExecution) {
          jobAttemptsFailed++;
          break;
        }
      }
    }
  }

  boolean hasErrors() {
    return jobAttemptsFailed > 0;
  }

  void printLastErrors() {
    LOGGER.warn("Job Attempt failures: [{}].", jobAttemptsFailed);
    for (ExternalCleaner externalCleaner : externalCleaners) {
      final String cleanerName = externalCleaner.getName();
      LOGGER.warn(
          "Failed to delete {} items using the cleaner {}",
          cleanerName,
          externalCleaner.getFailedCounter());
      for (String error : externalCleaner.getLastErrors()) {
        LOGGER.warn("[{}] {}", cleanerName, error);
      }
    }
  }

  String getReport() {
    final StringBuilder sb = new StringBuilder();
    sb.append(System.lineSeparator())
        .append("\tJobAttempts: ")
        .append(jobAttemptExecution)
        .append(", Attempts with failure: ")
        .append(jobAttemptsFailed);

    for (ExternalCleaner externalCleaner : externalCleaners) {
      final String cleanerName = externalCleaner.getName();
      long failures = externalCleaner.getFailedCounter();
      long total = externalCleaner.getSuccessCounter() + failures;
      sb.append(System.lineSeparator())
          .append("\t")
          .append(cleanerName)
          .append(" executions: ")
          .append(total)
          .append(", failures: ")
          .append(failures);
    }
    sb.append(System.lineSeparator());

    return sb.toString();
  }
}
