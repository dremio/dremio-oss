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
package com.dremio.dac.model.job;

import java.util.List;

import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

/**
 * Helper methods for building attempt details summary/reason
 */
public class AttemptsHelper {

  @VisibleForTesting
  public static final String OUT_OF_MEMORY_TEXT = "insufficient memory";

  @VisibleForTesting
  public static final String SCHEMA_CHANGE_TEXT = "schema learning";

  static String constructAttemptReason(AttemptReason reason) {
    if (reason == null) {
      return "";
    }

    switch (reason) {
      case OUT_OF_MEMORY:
        return OUT_OF_MEMORY_TEXT;
      case SCHEMA_CHANGE:
        return SCHEMA_CHANGE_TEXT;
      default:
        return "";
    }
  }

  static List<AttemptDetailsUI> fromAttempts(JobId jobId, List<JobAttempt> attempts) {
    List<AttemptDetailsUI> attemptDetails = Lists.newArrayList();
    int attemptIndex = 0;
    for (JobAttempt jobAttempt : attempts) {
      attemptDetails.add(new AttemptDetailsUI(jobAttempt, jobId, attemptIndex));
      attemptIndex++;
    }
    return attemptDetails;
  }

  static String constructSummary(List<JobAttempt> attempts) {
    if (attempts.size() == 1) {
      return "";
    }

    int memoryIssues = 0;
    int schemaIssues = 0;
    for (JobAttempt attempt : attempts) {
      if (attempt.getReason() == AttemptReason.OUT_OF_MEMORY) {
        memoryIssues++;
      } else if (attempt.getReason() == AttemptReason.SCHEMA_CHANGE) {
        schemaIssues++;
      }
    }

    return constructSummary(attempts.size(), memoryIssues, schemaIssues);
  }

  @VisibleForTesting
  public static String constructSummary(int numAttempts, int memoryIssues, int schemaIssues) {
    StringBuilder sb = new StringBuilder();
    sb.append("This query was attempted ").append(numAttempts).append(" times due to ");
    if (schemaIssues == 0) {
      sb.append(OUT_OF_MEMORY_TEXT);
    } else if (memoryIssues == 0) {
      sb.append(SCHEMA_CHANGE_TEXT);
    } else {
      sb.append(OUT_OF_MEMORY_TEXT).append(" (").append(memoryIssues)
              .append(") and ").append(SCHEMA_CHANGE_TEXT).append(" (").append(schemaIssues).append(")");
    }

    return sb.toString();
  }
}
