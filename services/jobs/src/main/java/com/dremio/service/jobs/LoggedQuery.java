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

import com.dremio.service.job.proto.JobState;

/**
 * Logged query for queries.json
 */
public class LoggedQuery {
  private final String queryId;
  private final String schema;
  private final String queryText;
  private final Long start;
  private final Long finish;
  private final JobState outcome;
  private final String username;

  LoggedQuery(String queryId, String schema, String queryText, Long start, Long finish, JobState outcome, String username) {
    this.queryId = queryId;
    this.schema = schema;
    this.queryText = queryText;
    this.start = start;
    this.finish = finish;
    this.outcome = outcome;
    this.username = username;
  }

  public String getQueryId() {
    return queryId;
  }

  public String getSchema() {
    return schema;
  }

  public String getQueryText() {
    return queryText;
  }

  public Long getStart() {
    return start;
  }

  public Long getFinish() {
    return finish;
  }

  public JobState getOutcome() {
    return outcome;
  }

  public String getUsername() {
    return username;
  }
}
