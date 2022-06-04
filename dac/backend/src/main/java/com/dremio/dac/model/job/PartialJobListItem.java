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

import static com.dremio.service.accelerator.AccelerationDetailsUtils.deserialize;

import java.util.Optional;

import com.dremio.dac.util.JobUtil;
import com.dremio.dac.util.TruncateString200Converter;
import com.dremio.proto.model.attempts.RequestType;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobsProtoUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * Represents a socket update to one job in jobs list
 */
@JsonIgnoreProperties(value={"isComplete"}, allowGetters=true)
public class PartialJobListItem {
  private final String id;
  private final JobState state;
  private final JobFailureInfo failureInfo;
  private final JobCancellationInfo cancellationInfo;
  private final String user;
  private final Long startTime;
  private final Long endTime;
  private final String description;
  private final boolean accelerated;
  private final boolean snowflakeAccelerated;
  private final RequestType requestType;
  private final String datasetVersion;
  private final boolean isComplete;
  private final boolean spilled;
  private final Long outputRecords;
  private final Boolean outputLimited;

  @JsonCreator
  public PartialJobListItem(
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
      @JsonProperty("outputRecords") long outputRecords,
      @JsonProperty("outputLimited") boolean outputLimited) {
    super();
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
    this.isComplete = isComplete(this.state);
    this.snowflakeAccelerated = snowflakeAccelerated;
    this.spilled = spilled;
    this.outputRecords = outputRecords;
    this.outputLimited = outputLimited;
  }

  public PartialJobListItem(Job input) {
    final JobAttempt firstAttempt = input.getAttempts().get(0);
    final JobAttempt lastAttempt = input.getAttempts().get(input.getAttempts().size() - 1);

    this.id = input.getJobId().getId();
    this.state = JobUtil.computeJobState(lastAttempt.getState(), input.isCompleted());
    this.failureInfo = JobDetailsUI.toJobFailureInfo(lastAttempt.getInfo().getFailureInfo(), lastAttempt.getInfo().getDetailedFailureInfo());
    this.cancellationInfo = JobDetailsUI.toJobCancellationInfo(lastAttempt.getState(), lastAttempt.getInfo().getCancellationInfo());
    this.user = firstAttempt.getInfo().getUser();
    this.startTime = firstAttempt.getInfo().getStartTime();
    this.endTime = lastAttempt.getInfo().getFinishTime();
    this.description = firstAttempt.getInfo().getDescription();
    this.accelerated = lastAttempt.getInfo().getAcceleration() != null;
    this.requestType =  firstAttempt.getInfo().getRequestType();
    this.datasetVersion = firstAttempt.getInfo().getDatasetVersion();
    this.isComplete = isComplete(state);
    final AccelerationDetails accelerationDetails = deserialize(lastAttempt.getAccelerationDetails());
    this.snowflakeAccelerated = this.accelerated && JobDetailsUI.wasSnowflakeAccelerated(accelerationDetails);
    this.spilled = lastAttempt.getInfo().getSpillJobDetails() != null;

    final JobStats stats = lastAttempt.getStats();
    this.outputRecords = Optional.ofNullable(stats).map(JobStats::getOutputRecords).orElse(null);
    this.outputLimited = Optional.ofNullable(stats).map(JobStats::getIsOutputLimited).orElse(false);
  }

  public PartialJobListItem(JobSummary input) {
    this.id = input.getJobId().getId();
    this.state = JobUtil.computeJobState(JobsProtoUtil.toStuff(input.getJobState()), input.getJobCompleted());
    this.failureInfo = JobDetailsUI.toJobFailureInfo(Strings.isNullOrEmpty(input.getFailureInfo()) ? null : input.getFailureInfo(),
      JobsProtoUtil.toStuff(input.getDetailedJobFailureInfo()));
    this.cancellationInfo = JobDetailsUI.toJobCancellationInfo(JobsProtoUtil.toStuff(input.getJobState()),
      JobsProtoUtil.toStuff(input.getCancellationInfo()));
    this.user = input.getUser();
    this.startTime = input.getStartTime() == 0 ? null : input.getStartTime();
    this.endTime = input.getEndTime() == 0 ? null : input.getEndTime();
    this.description = Strings.isNullOrEmpty(input.getDescription()) ? null : input.getDescription();
    this.accelerated = input.getAccelerated();
    this.requestType = JobsProtoUtil.toStuff(input.getRequestType());
    this.datasetVersion = input.getDatasetVersion();
    this.isComplete = isComplete(state);
    this.snowflakeAccelerated = input.getSnowflakeAccelerated();
    this.spilled = input.getSpilled();
    this.outputRecords = input.getOutputRecords();
    this.outputLimited = input.getOutputLimited();
  }

  private boolean isComplete(JobState state) {
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
}
