/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

/**
 * Attempt details that will be sent to the UI.
 */
public class AttemptDetailsUI {
  private final String reason;
  private final JobState result;
  private final String profileUrl;
  private final Long planningTime;
  private final Long enqueuedTime;
  private final Long executionTime;

  @JsonCreator
  public AttemptDetailsUI(
      @JsonProperty("reason") String reason,
      @JsonProperty("result") JobState result,
      @JsonProperty("profileUrl") String profileUrl,
      @JsonProperty("planningTime") Long planningTime,
      @JsonProperty("enqueuedTime") Long enqueuedTime,
      @JsonProperty("executionTime") Long executionTime) {
    this.reason = reason;
    this.result = result;
    this.profileUrl = profileUrl;
    this.planningTime = planningTime;
    this.enqueuedTime = enqueuedTime;
    this.executionTime = executionTime;
  }

  public AttemptDetailsUI(final JobAttempt jobAttempt, final JobId jobId, final int attemptIndex) {
    reason = AttemptsUIHelper.constructAttemptReason(jobAttempt.getReason());
    result = jobAttempt.getState();
    profileUrl = "/profiles/" + jobId.getId() + "?attempt=" + attemptIndex;
    enqueuedTime = AttemptsHelper.getEnqueuedTime(jobAttempt);
    planningTime = AttemptsHelper.getPlanningTime(jobAttempt);
    executionTime = AttemptsHelper.getExecutionTime(jobAttempt);
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

  public Long getPlanningTime() {
    return planningTime;
  }

  public Long getEnqueuedTime() {
    return enqueuedTime;
  }

  public Long getExecutionTime() {
    return executionTime;
  }
}
