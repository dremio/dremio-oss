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
package com.dremio.dac.explore.model;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.resource.JobResource;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Initial response to dataset preview request. Contains initial data and pagination URL to fetch remaining data.
 */
public class InitialDataPreviewResponse {

  private final JobDataFragment data;
  private final String paginationUrl;

  @JsonCreator
  public InitialDataPreviewResponse(
      @JsonProperty("data") JobDataFragment data,
      @JsonProperty("paginationUrl") String paginationUrl) {
    this.data = data;
    this.paginationUrl = paginationUrl;
  }

  public static InitialDataPreviewResponse of(JobDataFragment data) {
    return new InitialDataPreviewResponse(data, JobResource.getPaginationURL(data.getJobId()));
  }

  /**
   * Get the initial data returned with the response.
   * @return
   */
  public JobDataFragment getData() {
    return data;
  }

  /**
   * Get the pagination url to fetch remaining data from dataset.
   * @return
   */
  public String getPaginationUrl() {
    return paginationUrl;
  }
}
