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

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.jobs.AttemptsHelper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Attempt details that will be sent to the UI. */
public class AttemptDetailsUI {
  private final String reason;
  private final JobState result;
  private final String profileUrl;
  private final Long executionTime;
  private final Long commandPoolWaitTime;
  private final Long pendingTime;
  private final Long metadataRetrievalTime;
  private final Long planningTime;
  private final Long queuedTime;
  private final Long engineStartTime;
  private final Long executionPlanningTime;
  private final Long startingTime;
  private final Long runningTime;
  private final Long totalTime;

  @JsonCreator
  public AttemptDetailsUI(
      @JsonProperty("reason") String reason,
      @JsonProperty("result") JobState result,
      @JsonProperty("profileUrl") String profileUrl,
      @JsonProperty("executionTime") Long executionTime,
      @JsonProperty("commandPoolWaitTime") Long commandPoolWaitTime,
      @JsonProperty("pendingTime") Long pendingTime,
      @JsonProperty("metadataRetrievalTime") Long metadataRetrievalTime,
      @JsonProperty("planningTime") Long planningTime,
      @JsonProperty("queuedTime") Long queuedTime,
      @JsonProperty("engineStartTime") Long engineStartTime,
      @JsonProperty("executionPlanningTime") Long executionPlanningTime,
      @JsonProperty("startingTime") Long startingTime,
      @JsonProperty("runningTime") Long runningTime,
      @JsonProperty("totalTime") Long totalTime) {
    this.reason = reason;
    this.result = result;
    this.profileUrl = profileUrl;
    this.executionTime = executionTime;
    this.commandPoolWaitTime = commandPoolWaitTime;
    this.pendingTime = pendingTime;
    this.metadataRetrievalTime = metadataRetrievalTime;
    this.planningTime = planningTime;
    this.queuedTime = queuedTime;
    this.engineStartTime = engineStartTime;
    this.executionPlanningTime = executionPlanningTime;
    this.startingTime = startingTime;
    this.runningTime = runningTime;
    this.totalTime = totalTime;
  }

  public AttemptDetailsUI(final JobAttempt jobAttempt, final JobId jobId, final int attemptIndex) {
    AttemptsHelper attemptsHelper = new AttemptsHelper(jobAttempt);
    reason = AttemptsUIHelper.constructAttemptReason(jobAttempt.getReason());
    result = jobAttempt.getState();
    profileUrl = "/profiles/" + jobId.getId() + "?attempt=" + attemptIndex;

    commandPoolWaitTime = AttemptsHelper.getCommandPoolWaitTime(jobAttempt);
    if (attemptsHelper.hasStateDurations()) {
      pendingTime = attemptsHelper.getPendingTime();
      metadataRetrievalTime = attemptsHelper.getMetadataRetrievalTime();
      planningTime = attemptsHelper.getPlanningTime();
      queuedTime = attemptsHelper.getQueuedTime();
      engineStartTime = attemptsHelper.getEngineStartTime();
      executionPlanningTime = attemptsHelper.getExecutionPlanningTime();
      startingTime = attemptsHelper.getStartingTime();
      runningTime = attemptsHelper.getRunningTime();
      executionTime = runningTime;
      totalTime = attemptsHelper.getTotalTime();
    } else {
      // legacy jobs
      pendingTime = null;
      metadataRetrievalTime = null;
      planningTime = AttemptsHelper.getLegacyPlanningTime(jobAttempt);
      queuedTime = AttemptsHelper.getLegacyEnqueuedTime(jobAttempt);
      executionTime = AttemptsHelper.getLegacyExecutionTime(jobAttempt);
      engineStartTime = null;
      executionPlanningTime = null;
      startingTime = null;
      runningTime = executionTime;
      totalTime = planningTime + queuedTime + runningTime;
    }
  }

  public String getReason() {
    return reason;
  }

  public JobState getResult() {
    return result;
  }

  public String getProfileUrl() {
    return profileUrl;
  }

  public Long getExecutionTime() {
    return executionTime;
  }

  public Long getCommandPoolWaitTime() {
    return commandPoolWaitTime;
  }

  public Long getPendingTime() {
    return pendingTime;
  }

  public Long getMetadataRetrievalTime() {
    return metadataRetrievalTime;
  }

  public Long getPlanningTime() {
    return planningTime;
  }

  public Long getQueuedTime() {
    return queuedTime;
  }

  public Long getEngineStartTime() {
    return engineStartTime;
  }

  public Long getExecutionPlanningTime() {
    return executionPlanningTime;
  }

  public Long getStartingTime() {
    return startingTime;
  }

  public Long getRunningTime() {
    return runningTime;
  }

  public Long getTotalTime() {
    return totalTime;
  }
}
