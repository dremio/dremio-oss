/*
 * Copyright (C) 2017 Dremio Corporation
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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Initial run response when running run after previous transform.
 */
@JsonIgnoreProperties(value={"approximate"}, allowGetters=true)
public class InitialRunResponse {

  private final DatasetUI dataset;
  private final String paginationUrl;
  private final JobId jobId;
  private final History history;

  @JsonCreator
  public InitialRunResponse(
      @JsonProperty("dataset") DatasetUI dataset,
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("jobId") JobId jobId,
      @JsonProperty("history") History history) {
    super();
    this.dataset = dataset;
    this.paginationUrl = paginationUrl;
    this.jobId = jobId;
    this.history = history;
  }

  public static InitialRunResponse of(DatasetUI dataset, JobId jobId, History history) {
    return new InitialRunResponse(dataset, JobResource.getPaginationURL(jobId), jobId, history);
  }

  /**
   * Minimal Dataset information.
   * @return
   */
  public DatasetUI getDataset() {
    return dataset;
  }

  public String getPaginationUrl() {
    return paginationUrl;
  }

  public JobId getJobId() {
    return jobId;
  }

  public History getHistory() {
    return history;
  }

  public boolean isApproximate() {
    return false;
  }
}
