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
import com.dremio.dac.server.ApiErrorModel;
import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The result of a transform apply (preview result)
 */
public class InitialPreviewResponse {
  public static final int INITIAL_RESULTSET_SIZE = 50;

  private final DatasetUI dataset;
  private final JobDataFragment data;
  private final boolean isApproximate;
  private final String paginationUrl;
  private final History history;
  private final JobId jobId;
  // initial preview can fail when the source being queried has become unavailable
  private final ApiErrorModel error;

  @JsonCreator
  public InitialPreviewResponse(
      @JsonProperty("dataset") DatasetUI dataset,
      @JsonProperty("data") JobDataFragment data,
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("approximate") boolean isApproximate,
      @JsonProperty("jobId") JobId jobId,
      @JsonProperty("history") History history,
      @JsonProperty("error") ApiErrorModel error) {
    this.dataset = dataset;
    this.data = data;
    this.isApproximate = isApproximate;
    this.paginationUrl = paginationUrl;
    this.history = history;
    this.jobId = jobId;
    this.error = error;
  }

  public static InitialPreviewResponse of(DatasetUI dataset, JobDataFragment data, boolean isApproximate,
      History history, ApiErrorModel error) {
    if (data == null) {
      return of(dataset, isApproximate, history, error);
    } else {
      return new InitialPreviewResponse(dataset, data, JobResource.getPaginationURL(data.getJobId()),
        isApproximate, data.getJobId(), history, error);
    }
  }

  public static InitialPreviewResponse of(DatasetUI dataset, boolean isApproximate,
                                          History history, ApiErrorModel error) {
    return new InitialPreviewResponse(dataset, null, null,
      isApproximate, null, history, error);
  }

  /**
   * Get job id for preview.
   * @return
   */
  public JobId getJobId(){
    return jobId;
  }

  /**
   * Minimal Dataset information.
   * @return
   */
  public DatasetUI getDataset() {
    return dataset;
  }

  /**
   * Initial data. Currently it contains at max {@link #INITIAL_RESULTSET_SIZE} number of rows.
   * @return
   */
  public JobDataFragment getData() {
    return data;
  }

  /**
   * Pagination URL to fetch further records in job results. Ex value: job/{jobId}/data
   * @return
   */
  public String getPaginationUrl() {
    return paginationUrl;
  }

  /**
   * Are the results approximate (based on sampled data)?
   * @return
   */
  public boolean isApproximate() {
    return isApproximate;
  }

  /**
   * List of history items on dataset.
   * @return
   */
  public History getHistory() {
    return history;
  }

  /**
   * Error message if the preview query was unsuccessful.
   * @return
   */
  public ApiErrorModel getError() {
    return error;
  }
}
