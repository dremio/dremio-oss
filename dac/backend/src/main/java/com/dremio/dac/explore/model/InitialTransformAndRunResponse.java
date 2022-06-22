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
package com.dremio.dac.explore.model;

import com.dremio.dac.resource.JobResource;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.SessionId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response when a run and transformation are applied simultaneously. (Currently
 * only possible when modifying sql.)
 */
public class InitialTransformAndRunResponse {
  private final String paginationUrl;
  private final DatasetUI dataset;
  private final JobId jobId;
  private final SessionId sessionId;
  private final History history;

  @JsonCreator
  public InitialTransformAndRunResponse(
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("dataset") DatasetUI dataset,
      @JsonProperty("jobId") JobId jobId,
      @JsonProperty("sessionId") SessionId sessionId,
      @JsonProperty("history") History history) {
    super();
    this.paginationUrl = paginationUrl;
    this.dataset = dataset;
    this.jobId = jobId;
    this.sessionId = sessionId;
    this.history = history;
  }

  public static InitialTransformAndRunResponse of(DatasetUI dataset, JobId jobId, SessionId sessionId, History history) {
    return new InitialTransformAndRunResponse(JobResource.getPaginationURL(jobId), dataset, jobId, sessionId, history);
  }

  public String getPaginationUrl() {
    return paginationUrl;
  }

  public DatasetUI getDataset() {
    return dataset;
  }

  public JobId getJobId() {
    return jobId;
  }

  public SessionId getSessionId() {
    return sessionId;
  }

  public History getHistory() {
    return history;
  }

}
