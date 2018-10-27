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
package com.dremio.dac.api;

import java.util.ArrayList;
import java.util.List;

import com.dremio.service.accelerator.AccelerationDetailsUtils;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Job Status
 */
public class JobStatus {
  private final JobState jobState;
  private final Long rowCount;
  private final String errorMessage;
  @JsonISODateTime
  private final Long startedAt;
  @JsonISODateTime
  private final Long endedAt;
  private final JobAccelerationStatus acceleration;
  private final String queryType;
  private final String queueName;
  private final String queueId;
  @JsonISODateTime
  private final Long resourceSchedulingStartedAt;
  @JsonISODateTime
  private final Long resourceSchedulingEndedAt;

  @JsonCreator
  public JobStatus(
    @JsonProperty("jobState") JobState jobState,
    @JsonProperty("rowCount") Long rowCount,
    @JsonProperty("errorMessage") String errorMessage,
    @JsonProperty("startedAt") Long startedAt,
    @JsonProperty("endedAt") Long endedAt,
    @JsonProperty("acceleration") JobAccelerationStatus acceleration,
    @JsonProperty("queryType") String queryType,
    @JsonProperty("queueName") String queueName,
    @JsonProperty("queueId") String queueId,
    @JsonProperty("resourceSchedulingStartedAt") Long resourceSchedulingStartedAt,
    @JsonProperty("resourceSchedulingEndedAt") Long resourceSchedulingEndedAt) {
    this.jobState = jobState;
    this.rowCount = rowCount;
    this.errorMessage = errorMessage;
    this.startedAt = startedAt;
    this.endedAt = endedAt;
    this.acceleration = acceleration;
    this.queryType = queryType;
    this.queueName = queueName;
    this.queueId = queueId;
    this.resourceSchedulingStartedAt = resourceSchedulingStartedAt;
    this.resourceSchedulingEndedAt = resourceSchedulingEndedAt;
  }

  public JobState getJobState() {
    return jobState;
  }

  public Long getRowCount() {
    return rowCount;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public JobAccelerationStatus getAcceleration() {
    return acceleration;
  }

  public String getQueryType() {
    return queryType;
  }

  public String getQueueName() {
    return queueName;
  }

  public String getQueueId() {
    return queueId;
  }

  public Long getStartedAt() {
    return startedAt;
  }

  public Long getEndedAt() {
    return endedAt;
  }

  public Long getResourceSchedulingStartedAt() {
    return resourceSchedulingStartedAt;
  }

  public Long getResourceSchedulingEndedAt() {
    return resourceSchedulingEndedAt;
  }

  public static JobStatus fromJob(Job job) {
    JobState state = job.getJobAttempt().getState();
    String errorMessage = "";
    JobAccelerationStatus accelerationStatus = null;

    if (state == JobState.FAILED) {
      errorMessage = job.getJobAttempt().getInfo().getFailureInfo();
    } else if (state == JobState.COMPLETED) {
      AccelerationDetails details = AccelerationDetailsUtils.deserialize(job.getJobAttempt().getAccelerationDetails());
      if (details != null && details.getReflectionRelationshipsList() != null) {
        accelerationStatus = JobAccelerationStatus.fromAccelerationDetails(details);
      }
    }

    final ResourceSchedulingInfo rsi = job.getJobAttempt().getInfo().getResourceSchedulingInfo();
    final String queueName = rsi == null ? null : rsi.getQueueName();
    final String queueId = rsi == null ? null : rsi.getQueueId();
    final Long resourceSchedulingStartedAt = rsi == null ? null : rsi.getResourceSchedulingStart();
    final Long resourceSchedulingEndedAt = rsi == null ? null : rsi.getResourceSchedulingEnd();

    return new JobStatus(
      job.getJobAttempt().getState(),
      job.getJobAttempt().getDetails().getOutputRecords(),
      errorMessage,
      job.getJobAttempt().getInfo().getStartTime(),
      job.getJobAttempt().getInfo().getFinishTime(),
      accelerationStatus,
      job.getJobAttempt().getInfo().getQueryType().toString(),
      queueName,
      queueId,
      resourceSchedulingStartedAt,
      resourceSchedulingEndedAt
    );
  }

  /**
   * Acceleration status for a job
   */
  public static class JobAccelerationStatus {
    private final List<JobAccelerationRelationship> relationships;

    @JsonCreator
    public JobAccelerationStatus(
      @JsonProperty("relationships") List<JobAccelerationRelationship> relationships) {
      this.relationships = relationships;
    }

    public static JobAccelerationStatus fromAccelerationDetails(AccelerationDetails details) {
      List<JobAccelerationRelationship> relationships = new ArrayList<>();

      for (ReflectionRelationship relationship : details.getReflectionRelationshipsList()) {
        relationships.add(JobAccelerationRelationship.fromReflectionRelationShip(relationship));
      }

      return new JobAccelerationStatus(relationships);
    }

    public List<JobAccelerationRelationship> getReflectionRelationships() {
      return relationships;
    }
  }

  /**
   * Acceleration relationship for a job
   */
  public static class JobAccelerationRelationship {
    private final String datasetId;
    private final String reflectionId;
    private final String relationship;

    @JsonCreator
    public JobAccelerationRelationship(
      @JsonProperty("datasetId") String datasetId,
      @JsonProperty("reflectionId") String reflectionId,
      @JsonProperty("relationship") String relationship) {
      this.datasetId = datasetId;
      this.reflectionId = reflectionId;
      this.relationship = relationship;
    }

    public static JobAccelerationRelationship fromReflectionRelationShip(ReflectionRelationship relationship) {
      return new JobAccelerationRelationship(
        relationship.getDataset().getId(),
        relationship.getReflection().getId().getId(),
        relationship.getState().toString());
    }

    public String getRelationship() {
      return relationship;
    }

    public String getReflectionId() {
      return reflectionId;
    }

    public String getDatasetId() {
      return datasetId;
    }
  }

}
