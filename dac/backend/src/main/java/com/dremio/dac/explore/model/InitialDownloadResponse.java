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

import com.dremio.service.job.proto.JobId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Response to download request includes job id, url to retrieve job data. */
public class InitialDownloadResponse {

  private final JobId jobId;
  private final String downloadUrl;

  @JsonCreator
  public InitialDownloadResponse(
      @JsonProperty("jobId") JobId jobId, @JsonProperty("downloadUrl") String downloadUrl) {
    this.jobId = jobId;
    this.downloadUrl = downloadUrl;
  }

  public JobId getJobId() {
    return jobId;
  }

  public String getDownloadUrl() {
    return downloadUrl;
  }
}
