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

import com.dremio.service.job.JobSummary;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.DurationDetails;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents List of jobs returned as part of jobs list API
 */
public class JobListingItem extends PartialJobListingItem {

  @JsonCreator
  public JobListingItem(
    @JsonProperty("id") String id,
    @JsonProperty("queryType") QueryType queryType,
    @JsonProperty("queryUser") String queryUser,
    @JsonProperty("queryText") String queryText,
    @JsonProperty("plannerEstimatedCost") Double plannerEstimatedCost,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("engine") String engine,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("subEngine") String subEngine,
    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("wlmQueue") String wlmQueue,
    @JsonProperty("queriedDatasets") List<DataSet> queriedDatasets,
    @JsonProperty("isAccelerated") boolean isAccelerated,
    @JsonProperty("state") JobState state,
    @JsonProperty("startTime") Long startTime,
    @JsonProperty("endTime") Long endTime,
    @JsonProperty("duration") Long duration,
    @JsonProperty("durationDetails") List<DurationDetails> durationDetails,
    @JsonProperty("rowsScanned") Long rowsScanned,
    @JsonProperty("rowsReturned") Long rowsReturned,
    @JsonProperty("enqueuedTime") String enqueuedTime,
    @JsonProperty("waitOnClient") long waitInClient,
    @JsonProperty("input") String input,
    @JsonProperty("output") String output,
    @JsonProperty("spilled") boolean spilled,
    @JsonProperty("totalAttempts") int totalAttempts,
    @JsonProperty("isStarFlakeAccelerated") boolean isStarFlakeAccelerated,
    @JsonProperty("requestType") RequestType requestType,
    @JsonProperty("description") String description,
    @JsonProperty("isComplete") boolean isComplete,
    @JsonProperty("datasetVersion") String datasetVersion,
    @JsonProperty("outputLimited") boolean outputLimited
  ) {
    super(id, state, queryUser, startTime, endTime, duration, rowsScanned, rowsReturned, wlmQueue, queryText, queryType,
      isAccelerated, durationDetails, plannerEstimatedCost, queriedDatasets, engine, subEngine, enqueuedTime, waitInClient,
      input, output, spilled, totalAttempts, isStarFlakeAccelerated, requestType, description,isComplete,datasetVersion,outputLimited);
  }

  public JobListingItem(JobSummary input) {
    super(input);
  }
}
