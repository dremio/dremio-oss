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
package com.dremio.dac.api;

import com.dremio.service.jobs.Job;

/**
 * Query Job Results
 */
public class QueryJobResults {
  private final Job job;
  private final int offset;
  private final int limit;

  public QueryJobResults(Job job, int offset, int limit) {
    this.job = job;
    this.offset = offset;
    this.limit = limit;
  }

  public String getId() {
    return job.getJobId().getId();
  }

  public JobData getData() {
    return new JobData(job, offset, limit);
  }
}
