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

import static com.google.common.base.Preconditions.checkNotNull;

import com.dremio.dac.util.JSONUtil;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * History item for the history vertical bar
 */
@JsonIgnoreProperties(value={ "datasetVersion" }, allowGetters=true)
public class HistoryItem {
  /** path to that version of the dataset */
  private DatasetVersionResourcePath versionedResourcePath;
  /** status of the query */
  private JobState state;
  /** description of the transformation that produced this item */
  private String transformDescription;
  /** owner of that job */
  private String owner;
  /** time of the creation timestamp in ms*/
  private long createdAt;
  /** optional: time of the query completion timestamp in ms*/
  private Long finishedAt;
  /** whether this was a preview */
  private boolean preview;
  /** optional: number of records returned. (so far if STARTED )*/
  private Long recordsReturned;
  /** optional: size in bytes returned. (so far if STARTED )*/
  private Long bytes;

  @JsonCreator
  public HistoryItem(
      @JsonProperty("versionedResourcePath") DatasetVersionResourcePath versionedResourcePath,
      @JsonProperty("state") JobState state,
      @JsonProperty("transformDescription") String transformDescription,
      @JsonProperty("owner") String owner,
      @JsonProperty("createdAt") long createdAt,
      @JsonProperty("finishedAt") Long finishedAt,
      @JsonProperty("preview") boolean preview,
      @JsonProperty("recordsReturned") Long recordsReturned,
      @JsonProperty("bytes") Long bytes) {
    super();
    this.versionedResourcePath = checkNotNull(versionedResourcePath);
    this.state = state;
    this.transformDescription = transformDescription;
    this.owner = owner;
    this.createdAt = createdAt;
    this.finishedAt = finishedAt;
    this.preview = preview;
    this.recordsReturned = recordsReturned;
    this.bytes = bytes;
  }
  public DatasetVersionResourcePath getVersionedResourcePath() {
    return versionedResourcePath;
  }

  public DatasetVersion getDatasetVersion() {
    return versionedResourcePath.getVersion();
  }

  @JsonIgnore
  public DatasetPath getDataset() {
    return versionedResourcePath.getDataset();
  }
  public JobState getState() {
    return state;
  }
  public String getTransformDescription() {
    return transformDescription;
  }
  public String getOwner() {
    return owner;
  }
  public long getCreatedAt() {
    return createdAt;
  }
  public boolean isPreview() {
    return preview;
  }
  public Long getRecordsReturned() {
    return recordsReturned;
  }
  public Long getFinishedAt() {
    return finishedAt;
  }
  public Long getBytes() {
    return bytes;
  }

  @Override
  public String toString() {
    return JSONUtil.toString(this);
  }
}
