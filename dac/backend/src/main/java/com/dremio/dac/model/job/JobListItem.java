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
package com.dremio.dac.model.job;

import java.util.List;

import com.dremio.proto.model.attempts.RequestType;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.Job;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents one job returned as part of jobs list
 */
public class JobListItem extends PartialJobListItem {
  private final DatasetType datasetType;
  private final List<String> datasetPathList;

  @JsonCreator
  public JobListItem(
      @JsonProperty("id") String id,
      @JsonProperty("state") JobState state,
      @JsonProperty("failureInfo") JobFailureInfo failureInfo,
      @JsonProperty("cancellationInfo") JobCancellationInfo cancellationInfo,
      @JsonProperty("user") String user,
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("endTime") Long endTime,
      @JsonProperty("description") String description,
      @JsonProperty("datasetPathList") List<String> datasetPathList,
      @JsonProperty("datasetType") DatasetType datasetType,
      @JsonProperty("requestType") RequestType requestType,
      @JsonProperty("accelerated") boolean accelerated,
      @JsonProperty("datasetVersion") String datasetVersion,
      @JsonProperty("snowflakeAccelerated") boolean snowflakeAccelerated,
      @JsonProperty("spilled") boolean spilled) {
    super(id, state, failureInfo, cancellationInfo, user, startTime, endTime, description, requestType,
        accelerated, datasetVersion, snowflakeAccelerated, spilled);
    this.datasetPathList = datasetPathList;
    this.datasetType = datasetType;
  }

  public JobListItem(Job input, ParentDatasetInfo displayInfo) {
    super(input);
    this.datasetPathList = displayInfo.getDatasetPathList();
    this.datasetType =  displayInfo.getType();
  }

  public List<String> getDatasetPathList() {
    return datasetPathList;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

}
