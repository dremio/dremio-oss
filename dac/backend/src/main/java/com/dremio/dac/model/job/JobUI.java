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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.proto.model.job.JobAttemptUI;
import com.dremio.dac.proto.model.job.JobInfoUI;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.JobsServiceUtil;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

/**
 * Job represents details of a currently running or completed query on a dataset.
 *
 * This class is a wrapper around {@code Job} targeted for UI consumption
 */
public class JobUI {
  private final JobId jobId;
  private final SessionId sessionId;
  private final List<JobAttemptUI> attempts;
  private final JobData data;

  public JobUI(JobsService jobsService, JobId jobId, SessionId sessionId, String userName) {
    this.jobId = jobId;
    this.sessionId = sessionId;

    JobDetailsRequest jobDetailsRequest = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setUserName(userName)
      .build();
    try {
      JobDetails jobDetails = jobsService.getJobDetails(jobDetailsRequest);
      this.attempts = jobDetails.getAttemptsList().stream()
        .map(input -> toUI(JobsProtoUtil.toStuff(input)))
        .collect(Collectors.toList());
    } catch (JobNotFoundException e) {
      throw new IllegalArgumentException("Invalid JobId");
    }
    this.data = new JobDataWrapper(jobsService, jobId, null, userName);
  }

  @JsonCreator
  public JobUI(@JsonProperty("jobId") JobId jobId,
               @JsonProperty("sessionId") SessionId sessionId,
               @JsonProperty("jobAttempt") JobAttemptUI jobConfig) {
    this.jobId = jobId;
    this.sessionId = sessionId;
    this.attempts = ImmutableList.of(checkNotNull(jobConfig, "jobAttempt is null"));
    this.data = null;
  }

  public JobId getJobId() {
    return jobId;
  }

  public SessionId getSessionId() {
    return sessionId;
  }

  public JobAttemptUI getJobAttempt() {
    checkState(attempts.size() >=1, "There should be at least one attempt in JobUI");
    int lastAttempt = attempts.size() - 1;
    return attempts.get(lastAttempt);
  }


  @Override
  public String toString() {
    final JobAttemptUI jobAttempt = getJobAttempt();
    final String sessionIdStr = sessionId == null ? null : sessionId.getId();
    return format("{JobId: %s, SessionId: %s, SQL: %s, Dataset: %s, DatasetVersion: %s}",
            getJobId(), sessionIdStr, jobAttempt.getInfo().getSql(),
            PathUtils.constructFullPath(jobAttempt.getInfo().getDatasetPathList()),
            jobAttempt.getInfo().getDatasetVersion()); //todo
  }

  @JsonIgnore
  public JobData getData(){
    checkState(data != null, "not available from deserialized Job");
    return data;
  }

  @Override
  public int hashCode() {
    return Objects.hash(jobId, attempts);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof JobUI) {
        JobUI other = (JobUI) obj;
        return Objects.equals(jobId, other.jobId) && Objects.equals(attempts, other.attempts);
      }
    }
    return false;
  }

  private static JobInfoUI convertJobInfo(JobInfo info) {
    return new JobInfoUI()
      .setJobId(info.getJobId())
      .setSql(info.getSql())
      .setRequestType(info.getRequestType())
      .setClient(info.getClient())
      .setUser(info.getUser())
      .setStartTime(info.getStartTime())
      .setFinishTime(info.getFinishTime())
      .setDatasetPathList(info.getDatasetPathList())
      .setDatasetVersion(info.getDatasetVersion())
      .setSpace(info.getSpace())
      .setParentsList(info.getParentsList())
      .setQueryType(info.getQueryType())
      .setAppId(info.getAppId())
      .setFailureInfo(info.getFailureInfo())
      .setDetailedFailureInfo(info.getDetailedFailureInfo())
      .setFieldOriginsList(info.getFieldOriginsList())
      .setResultMetadataList(info.getResultMetadataList())
      .setAcceleration(info.getAcceleration())
      .setGrandParentsList(info.getGrandParentsList())
      .setDownloadInfo(info.getDownloadInfo())
      .setDescription(JobsServiceUtil.getJobDescription(info.getRequestType(), info.getSql(), info.getDescription()))
      .setMaterializationFor(info.getMaterializationFor())
      .setOriginalCost(info.getOriginalCost())
      .setPartitionsList(info.getPartitionsList())
      .setScanPathsList(info.getScanPathsList())
      .setJoinAnalysis(info.getJoinAnalysis())
      .setContextList(info.getContextList())
      .setResourceSchedulingInfo(info.getResourceSchedulingInfo())
      .setOutputTableList(info.getOutputTableList())
      .setCancellationInfo(info.getCancellationInfo())
      .setSpillJobDetails(info.getSpillJobDetails());

  }

  private static JobAttemptUI toUI(JobAttempt attempt) {
    if (attempt == null) {
      return null;
    }
    return new JobAttemptUI()
      .setState(attempt.getState())
      .setInfo(convertJobInfo(attempt.getInfo()))
      .setStats(attempt.getStats())
      .setDetails(attempt.getDetails())
      .setReason(attempt.getReason())
      .setAttemptId(attempt.getAttemptId())
      .setEndpoint(attempt.getEndpoint());
  }
}
