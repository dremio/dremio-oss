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

import com.dremio.service.job.log.JobState;
import com.dremio.service.job.log.LoggedQuery;
import com.dremio.service.job.proto.JobInfo;
import java.util.List;
import java.util.function.Function;

/** Converts a job to a log entry */
public class JobResultToLogEntryConverter implements Function<Job, LoggedQuery> {
  @Override
  public LoggedQuery apply(Job job) {
    final JobInfo info = job.getJobAttempt().getInfo();
    final List<String> contextList = info.getContextList();
    String outcomeReason = null;
    switch (job.getJobAttempt().getState()) {
      case CANCELED:
        outcomeReason = info.getCancellationInfo().getMessage();
        break;
      case FAILED:
        outcomeReason = info.getFailureInfo();
        break;
      default:
        break;
    }

    final LoggedQuery.Builder builder = LoggedQuery.newBuilder();
    if (job.getJobId() != null) {
      builder.setQueryId(job.getJobId().getId());
    }
    if (info.getStartTime() != null) {
      builder.setStart(info.getStartTime());
    }
    if (info.getSql() != null) {
      builder.setQueryText(info.getSql());
      builder.setIsTruncatedQueryText(info.getIsTruncatedSql());
    }
    if (info.getFinishTime() != null) {
      builder.setFinish(info.getFinishTime());
    }
    if (job.getJobAttempt().getState() != null) {
      builder.setOutcome(JobState.valueOf(job.getJobAttempt().getState().name()));
    }
    if (outcomeReason != null) {
      builder.setOutcomeReason(outcomeReason);
    }
    if (info.getUser() != null) {
      builder.setUsername(info.getUser());
    }
    if (contextList != null) {
      builder.setContext(contextList.toString());
    }

    return builder.build();
  }
}
