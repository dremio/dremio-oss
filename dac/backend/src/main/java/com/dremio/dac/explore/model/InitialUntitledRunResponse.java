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
import java.util.List;

/** Initial run response when running run after previous transform. */
public class InitialUntitledRunResponse {

  private final List<String> datasetPath;
  private final String datasetVersion;
  private final String paginationUrl;
  private final JobId jobId;
  private final SessionId sessionId;

  @JsonCreator
  public InitialUntitledRunResponse(
      @JsonProperty("datasetPath") List<String> datasetPath,
      @JsonProperty("datasetVersion") String datasetVersion,
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("jobId") JobId jobId,
      @JsonProperty("sessionId") SessionId sessionId) {
    super();
    this.datasetPath = datasetPath;
    this.datasetVersion = datasetVersion;
    this.paginationUrl = paginationUrl;
    this.jobId = jobId;
    this.sessionId = sessionId;
  }

  public static InitialUntitledRunResponse of(
      List<String> datasetPath, String datasetVersion, JobId jobId, SessionId sessionId) {
    return new InitialUntitledRunResponse(
        datasetPath, datasetVersion, JobResource.getPaginationURL(jobId), jobId, sessionId);
  }

  /**
   * Minimal Dataset information.
   *
   * @return
   */
  public List<String> getDatasetPath() {
    return datasetPath;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  public String getPaginationUrl() {
    return paginationUrl;
  }

  public JobId getJobId() {
    return jobId;
  }

  public SessionId getSessionId() {
    return sessionId;
  }
}
