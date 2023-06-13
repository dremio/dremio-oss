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

import java.util.ArrayList;
import java.util.List;

import com.dremio.dac.util.JobUtil;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.DurationDetails;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsServiceUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Initializing values to send as part of API response
 */
@JsonIgnoreProperties(value = {"isComplete"}, allowGetters = true)
public class PartialJobListingItem {
  private final String id;
  private final JobState state;
  private final String user;
  private final Long startTime;
  private final Long endTime;
  private final String queryText;
  private final boolean isAccelerated;
  private final QueryType queryType;
  private final long duration;
  private final long outputRecords;
  private final long rowsScanned;
  private String wlmQueue;
  private List<DurationDetails> durationDetails = new ArrayList<>();
  private final Double plannerEstimatedCost;
  private List<DataSet> queriedDatasets = new ArrayList<>();
  private final String engine;
  private final String subEngine;
  private String enqueuedTime;
  private long waitInClient;
  private String input;
  private String output;
  private boolean spilled;
  private int totalAttempts;
  private boolean isStarFlakeAccelerated;
  private RequestType requestType;
  private String description;
  private boolean isComplete;
  private final String datasetVersion;
  private final Boolean outputLimited;

  @JsonCreator
  public PartialJobListingItem(
    @JsonProperty("id") String id,
    @JsonProperty("state") JobState state,
    @JsonProperty("user") String user,
    @JsonProperty("startTime") Long startTime,
    @JsonProperty("endTime") Long endTime,
    @JsonProperty("duration") Long duration,
    @JsonProperty("rowsScanned") Long rowsScanned,
    @JsonProperty("outputRecords") Long outputRecords,
    @JsonProperty("wlmQueue") String wlmQueue,
    @JsonProperty("queryText") String queryText,
    @JsonProperty("queryType") QueryType queryType,
    @JsonProperty("isAccelerated") boolean isAccelerated,
    @JsonProperty("durationDetails") List<DurationDetails> durationDetails,
    @JsonProperty("plannerEstimatedCost") Double plannerEstimatedCost,
    @JsonProperty("queriedDatasets") List<DataSet> queriedDatasets,
    @JsonProperty("engine") String engine,
    @JsonProperty("subEngine") String subEngine,
    @JsonProperty("enqueuedTime") String enqueuedTime,
    @JsonProperty("waitOnClient") long waitInClient,
    @JsonProperty("input") String input,
    @JsonProperty("output") String output,
    @JsonProperty("spilled") boolean spilled,
    @JsonProperty("totalAttempts") int totalAttempts,
    @JsonProperty("output") boolean isStarFlakeAccelerated,
    @JsonProperty("requestType") RequestType requestType,
    @JsonProperty("description") String description,
    @JsonProperty("isComplete") boolean isComplete,
    @JsonProperty("datasetVersion") String datasetVersion,
    @JsonProperty("outputLimited") boolean outputLimited
    ) {
    super();
    this.id = id;
    this.state = state;
    this.user = user;
    this.startTime = startTime;
    this.endTime = endTime;
    this.duration = duration;
    this.rowsScanned = rowsScanned;
    this.outputRecords = outputRecords;
    this.queryText = queryText;
    this.isAccelerated = isAccelerated;
    this.queryType = queryType;
    this.wlmQueue = wlmQueue;
    this.durationDetails = durationDetails;
    this.plannerEstimatedCost = plannerEstimatedCost;
    this.queriedDatasets = queriedDatasets;
    this.engine = engine;
    this.subEngine = subEngine;
    this.enqueuedTime = enqueuedTime;
    this.waitInClient = waitInClient;
    this.input = input;
    this.output = output;
    this.spilled = spilled;
    this.totalAttempts = totalAttempts;
    this.isStarFlakeAccelerated = isStarFlakeAccelerated;
    this.requestType = requestType;
    this.description = description;
    this.isComplete = isComplete;
    this.datasetVersion = datasetVersion;
    this.outputLimited = outputLimited;
  }

  public PartialJobListingItem(JobSummary input) {
    this.id = input.getJobId().getId();
    this.user = input.getUser();
    int totalAttempts = (int) input.getNumAttempts();
    this.totalAttempts = totalAttempts;
    this.state = JobUtil.computeJobState(JobsProtoUtil.toStuff(input.getJobState()), input.getJobCompleted());
    this.startTime = input.getStartTime() != 0 ? input.getStartTime() : 0;
    this.endTime = input.getEndTime() != 0 ? input.getEndTime() : 0;
    this.isComplete = JobUtil.isComplete(this.state);
    this.duration = JobUtil.getTotalDuration(input, isComplete);
    this.durationDetails = JobUtil.buildDurationDetails(input.getStateListList());
    this.rowsScanned = input.getInputRecords();
    this.outputRecords = input.getOutputRecords();
    this.wlmQueue = input.getQueueName();
    this.queryText = input.getSql();
    this.isAccelerated = input.getAccelerated();
    this.queryType = JobsProtoUtil.toStuff(input.getQueryType());
    this.plannerEstimatedCost = input.getOriginalCost();
    this.engine = input.getEngine();
    this.subEngine = input.getSubEngine();
    this.queriedDatasets = JobUtil.buildQueriedDatasets(JobsProtoUtil.toStuffParentDatasetInfoList(input.getParentsList()), input.getRequestType(), input.getDatasetPathList());
    this.durationDetails.stream().filter(d -> d.getPhaseName().equalsIgnoreCase("QUEUED")).forEach(mp -> enqueuedTime = mp.getPhaseDuration());
    this.waitInClient = input.getWaitInclient();
    this.input = JobUtil.getConvertedBytes(input.getInputBytes()) + " / " +
      input.getInputRecords() + " Records";
    this.output = JobUtil.getConvertedBytes(input.getOutputBytes()) + " / " +
      input.getOutputRecords() + " Records";
    this.spilled = input.getSpilled();
    this.isStarFlakeAccelerated = input.getSnowflakeAccelerated();
    this.description = JobsServiceUtil.getJobDescription(input.getRequestType(), input.getSql(), input.getDescription());
    this.requestType = input.getRequestType();
    this.datasetVersion = input.getDatasetVersion();
    this.outputLimited = input.getOutputLimited();
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public String getDescription() {
    return description;
  }

  public boolean isStarFlakeAccelerated() {
    return isStarFlakeAccelerated;
  }

  public List<DurationDetails> getDurationDetails() {
    return durationDetails;
  }

  public long getDuration() {
    return duration;
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

  public Long getOutputRecords() {
    return outputRecords;
  }

  public Long getRowsScanned() {
    return rowsScanned;
  }

  public String getWlmQueue() {
    return wlmQueue;
  }

  public String getQueryText() {
    return queryText;
  }

  public boolean isAccelerated() {
    return isAccelerated;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public Double getPlannerEstimatedCost() {
    return plannerEstimatedCost;
  }

  public List<DataSet> getQueriedDatasets() {
    return queriedDatasets;
  }

  public String getEngine() {
    return engine;
  }

  public String getSubEngine() {
    return subEngine;
  }

  public String getEnqueuedTime() {
    return enqueuedTime;
  }

  public long getWaitInClient() {
    return waitInClient;
  }

  public String getInput() {
    return input;
  }

  public int getTotalAttempts() {
    return totalAttempts;
  }

  public String getOutput() {
    return output;
  }

  public boolean isSpilled() {
    return spilled;
  }

  @JsonProperty("isComplete")
  public boolean isComplete() {
    return isComplete;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  public Boolean getOutputLimited() {
    return outputLimited;
  }

  public void setWlmQueue(String wlmQueue) {
    this.wlmQueue = wlmQueue;
  }
}
