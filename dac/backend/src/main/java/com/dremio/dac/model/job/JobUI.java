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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import java.util.List;
import java.util.Objects;

import com.dremio.common.utils.PathUtils;
import com.dremio.dac.proto.model.job.JobAttemptUI;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Job represents details of a currently running or completed query on a dataset.
 *
 * This class is a wrapper around {@code Job} targeted for UI consumption
 */
public class JobUI {
  private final JobId jobId;
  private final List<JobAttemptUI> attempts;
  private final JobData data;

  public JobUI(Job job) {
    this.jobId = job.getJobId();
    this.attempts = FluentIterable.from(job.getAttempts())
      .transform(new Function<JobAttempt, JobAttemptUI>() {
        @Override
        public JobAttemptUI apply(JobAttempt input) {
          return toUI(input);
        }
      }).toList();
    this.data = new JobDataWrapper(job.getData());
  }

  @JsonCreator
  public JobUI(@JsonProperty("jobId") JobId jobId, @JsonProperty("jobAttempt") JobAttemptUI jobConfig) {
    this.jobId = jobId;
    this.attempts = ImmutableList.of(checkNotNull(jobConfig, "jobAttempt is null"));
    this.data = null;
  }

  public JobId getJobId() {
    return jobId;
  }

  public JobAttemptUI getJobAttempt() {
    checkState(attempts.size() >=1, "There should be at least one attempt in JobUI");
    int lastAttempt = attempts.size() - 1;
    return attempts.get(lastAttempt);
  }


  @Override
  public String toString() {
    final JobAttemptUI jobAttempt = getJobAttempt();
    return format("{JobId: %s, SQL: %s, Dataset: %s, DatasetVersion: %s}",
            getJobId(), jobAttempt.getInfo().getSql(),
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

  private static JobAttemptUI toUI(JobAttempt attempt) {
    if (attempt == null) {
      return null;
    }
    return new JobAttemptUI()
      .setState(attempt.getState())
      .setInfo(attempt.getInfo())
      .setStats(attempt.getStats())
      .setDetails(attempt.getDetails())
      .setReason(attempt.getReason())
      .setAttemptId(attempt.getAttemptId())
      .setEndpoint(attempt.getEndpoint());
  }
}

