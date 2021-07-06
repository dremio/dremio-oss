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

import com.dremio.dac.util.TruncateString200Converter;
import com.dremio.proto.model.attempts.RequestType;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Job summary sent to UI.
 */
@JsonIgnoreProperties(value={"isComplete"}, allowGetters=true)
public class JobSummaryUI {
  private final String id;
  private final JobState state;
  private final JobFailureInfo failureInfo;
  private final JobCancellationInfo cancellationInfo;
  private final String user;
  private final Long startTime;
  private final Long endTime;
  private final String description;
  private final RequestType requestType;
  private final boolean accelerated;
  private final String datasetVersion;
  private final boolean snowflakeAccelerated;
  private final boolean spilled;
  private final Long outputRecords;
  private final boolean outputLimited;
  private final Long processedRecords;
  private final List<String> datasetPathList;
  private final DatasetType datasetType;
  private final boolean isComplete;

  @JsonCreator
  public JobSummaryUI(
    @JsonProperty("id") String id,
    @JsonProperty("state") JobState state,
    @JsonProperty("failureInfo") JobFailureInfo failureInfo,
    @JsonProperty("cancellationInfo") JobCancellationInfo cancellationInfo,
    @JsonProperty("user") String user,
    @JsonProperty("startTime") Long startTime,
    @JsonProperty("endTime") Long endTime,
    @JsonProperty("description") String description,
    @JsonProperty("requestType") RequestType requestType,
    @JsonProperty("accelerated") boolean accelerated,
    @JsonProperty("datasetVersion") String datasetVersion,
    @JsonProperty("snowflakeAccelerated") boolean snowflakeAccelerated,
    @JsonProperty("spilled") boolean spilled,
    @JsonProperty("outputRecords") Long outputRecords,
    @JsonProperty("outputLimited") boolean outputLimited,
    @JsonProperty("processedRecords") Long processedRecords,
    @JsonProperty("datasetPathList") List<String> datasetPathList,
    @JsonProperty("datasetType") DatasetType datasetType) {
    this.id = id;
    this.state = state;
    this.failureInfo = failureInfo;
    this.cancellationInfo = cancellationInfo;
    this.user = user;
    this.startTime = startTime;
    this.endTime = endTime;
    this.description = description;
    this.accelerated = accelerated;
    this.requestType = requestType;
    this.datasetVersion = datasetVersion;
    this.snowflakeAccelerated = snowflakeAccelerated;
    this.spilled = spilled;
    this.outputRecords = outputRecords;
    this.outputLimited = outputLimited;
    this.processedRecords = processedRecords;
    this.datasetPathList = datasetPathList;
    this.datasetType = datasetType;
    this.isComplete = isComplete(this.state);
  }

  public static JobSummaryUI of(com.dremio.service.job.JobSummary input, NamespaceService service) {
    final ParentDatasetInfo datasetInfo = JobsUI.getDatasetToDisplay(input, service);
    return new JobSummaryUI(
      input.getJobId().getId(),
      JobsProtoUtil.toStuff(input.getJobState()),
      JobDetailsUI.toJobFailureInfo(
        Strings.isNullOrEmpty(input.getFailureInfo()) ? null : input.getFailureInfo(),
        JobsProtoUtil.toStuff(input.getDetailedJobFailureInfo())),
      JobDetailsUI.toJobCancellationInfo(JobsProtoUtil.toStuff(input.getJobState()),
        JobsProtoUtil.toStuff(input.getCancellationInfo())),
      input.getUser(),
      input.getStartTime() == 0 ? null : input.getStartTime(),
      input.getEndTime() == 0 ? null : input.getEndTime(),
      Strings.isNullOrEmpty(input.getDescription()) ? null : input.getDescription(),
      JobsProtoUtil.toStuff(input.getRequestType()),
      input.getAccelerated(),
      input.getDatasetVersion(),
      input.getSnowflakeAccelerated(),
      input.getSpilled(),
      input.getOutputRecords(),
      input.getOutputLimited(),
      input.getRecordCount(),
      datasetInfo.getDatasetPathList(),
      datasetInfo.getType());
  }

  private static boolean isComplete(JobState state) {
    Preconditions.checkNotNull(state, "JobState must be set");

    switch(state){
      case CANCELLATION_REQUESTED:
      case ENQUEUED:
      case NOT_SUBMITTED:
      case RUNNING:
      case STARTING:
      case PLANNING:
      case PENDING:
      case METADATA_RETRIEVAL:
      case QUEUED:
      case ENGINE_START:
      case EXECUTION_PLANNING:
        return false;
      case CANCELED:
      case COMPLETED:
      case FAILED:
        return true;
      default:
        throw new UnsupportedOperationException();
    }
  }

  public String getId() {
    return id;
  }

  public JobState getState() {
    return state;
  }

  public JobFailureInfo getFailureInfo() {
    return failureInfo;
  }

  public JobCancellationInfo getCancellationInfo() {
    return cancellationInfo;
  }

  public String getUser() {
    return user;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  @JsonSerialize(converter = TruncateString200Converter.class)
  public String getDescription() {
    return description;
  }

  public boolean isAccelerated() {
    return accelerated;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  @JsonProperty("isComplete")
  public boolean isComplete() {
    return isComplete;
  }

  public boolean isSnowflakeAccelerated() {
    return snowflakeAccelerated;
  }

  public boolean isSpilled() {
    return spilled;
  }

  public Long getOutputRecords() {
    return outputRecords;
  }

  public Boolean isOutputLimited() {
    return outputLimited;
  }

  public Long getProcessedRecords() {
    return processedRecords;
  }

  public List<String> getDatasetPathList() {
    return datasetPathList;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }
}
