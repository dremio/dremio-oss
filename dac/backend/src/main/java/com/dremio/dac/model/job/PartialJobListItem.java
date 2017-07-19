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
package com.dremio.dac.model.job;

import com.dremio.exec.proto.beans.RequestType;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a socket update to one job in jobs list
 */
@JsonIgnoreProperties(value={"isComplete"}, allowGetters=true)
public class PartialJobListItem {
  private final String id;
  private final JobState state;
  private final String user;
  private final Long startTime;
  private final Long endTime;
  private final String description;
  private final boolean accelerated;
  private final RequestType requestType;
  private final String datasetVersion;
  private final boolean isComplete;

  @JsonCreator
  public PartialJobListItem(
      @JsonProperty("id") String id,
      @JsonProperty("state") JobState state,
      @JsonProperty("user") String user,
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("endTime") Long endTime,
      @JsonProperty("description") String description,
      @JsonProperty("requestType") RequestType requestType,
      @JsonProperty("accelerated") boolean accelerated,
      @JsonProperty("datasetVersion") String datasetVersion) {
    super();
    this.id = id;
    this.state = state;
    this.user = user;
    this.startTime = startTime;
    this.endTime = endTime;
    this.description = description;
    this.accelerated = accelerated;
    this.requestType = requestType;
    this.datasetVersion = datasetVersion;
    this.isComplete = isComplete(state);
  }

  public PartialJobListItem(Job input) {
    final JobAttempt firstAttempt = input.getAttempts().get(0);
    final JobAttempt lastAttempt = input.getAttempts().get(input.getAttempts().size() - 1);

    this.id = input.getJobId().getId();
    this.state = lastAttempt.getState();
    this.user = firstAttempt.getInfo().getUser();
    this.startTime = firstAttempt.getInfo().getStartTime();
    this.endTime = lastAttempt.getInfo().getFinishTime();
    this.description = firstAttempt.getInfo().getDescription();
    this.accelerated = firstAttempt.getInfo().getAcceleration() != null;
    this.requestType =  firstAttempt.getInfo().getRequestType();
    this.datasetVersion = firstAttempt.getInfo().getDatasetVersion();
    this.isComplete = isComplete(state);
  }

  private boolean isComplete(JobState state){
    switch(state){
      case CANCELLATION_REQUESTED:
      case ENQUEUED:
      case NOT_SUBMITTED:
      case RUNNING:
      case STARTING:
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

  public String getUser() {
    return user;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

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

}
