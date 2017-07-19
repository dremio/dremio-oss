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

import java.util.List;

import com.dremio.dac.model.namespace.DatasetContainer;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Dataset information used in the right-hand panel on the UI in
 * a context associated with a particular dataset.
 */
public class DatasetDetails {

  private final List<String> displayFullPath;
  private final String owner;
  private final int jobCount;
  private final int descendants;
  private final Long createdAt;
  private final DatasetContainer parentDatasetContainer;

  public DatasetDetails(
      DatasetConfig datasetConfig,
      int jobCount,
      int descendants,
      DatasetContainer parentDatasetContainer
  ) {
    this.displayFullPath = datasetConfig.getFullPathList();
    this.owner = datasetConfig.getOwner();
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.createdAt = datasetConfig.getCreatedAt();
    this.parentDatasetContainer = parentDatasetContainer;
  }

  @JsonCreator
  public DatasetDetails(
      @JsonProperty("id") List<String> displayFullPath,
      @JsonProperty("owner") String owner,
      @JsonProperty("jobCount") int jobCount,
      @JsonProperty("descendants") int descendants,
      @JsonProperty("createdAt") Long createdAt,
      @JsonProperty("parentDatasetContainer") DatasetContainer parentDatasetContainer) {
    this.displayFullPath = displayFullPath;
    this.owner = owner;
    this.jobCount = jobCount;
    this.descendants = descendants;
    this.createdAt = createdAt;
    this.parentDatasetContainer = parentDatasetContainer;
  }

  /**
   * This entity is identified by the display full path of the dataset.
   *
   * @return - full path as list of path sections
   */
  public List<String> getId() {
    return displayFullPath;
  }

  public Integer getJobCount() {
    return jobCount;
  }

  public Integer getDescendants() {
    return descendants;
  }

  public String getOwner() {
    return owner;
  }

  public Long getCreatedAt() {
    return createdAt;
  }

  public DatasetContainer getParentDatasetContainer() {
    return parentDatasetContainer;
  }
}
