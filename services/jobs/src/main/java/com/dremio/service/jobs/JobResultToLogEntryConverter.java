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

import java.util.List;
import java.util.function.Function;

import com.dremio.service.job.proto.JobInfo;

/**
 * Converts a job to a log entry
 */
public class JobResultToLogEntryConverter implements Function<Job, LoggedQuery> {
  @Override
  public LoggedQuery apply(Job job) {
    final JobInfo info = job.getJobAttempt().getInfo();
    final List<String> contextList = info.getContextList();

    return new LoggedQuery(
      job.getJobId().getId(),
      contextList == null ? null : contextList.toString(),
      info.getSql(),
      info.getStartTime(),
      info.getFinishTime(),
      job.getJobAttempt().getState(),
      info.getUser()
    );
  }
}
