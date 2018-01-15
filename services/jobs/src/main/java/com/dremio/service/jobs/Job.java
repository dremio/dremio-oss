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
package com.dremio.service.jobs;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.dremio.common.utils.PathUtils;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Job represents details of a currently running or completed query on a dataset.
 */
public class Job {

  private final JobId jobId;
  private final List<JobAttempt> attempts = new CopyOnWriteArrayList<>();
  private final JobResultsStore resultsStore;

  private JobData data;

  public Job(JobId jobId, JobAttempt jobAttempt) {
    this.jobId = jobId;
    this.resultsStore = null;
    attempts.add( checkNotNull(jobAttempt, "jobAttempt is null"));
  }

  /**
   * Create an instance which loads the job results lazily.
   *
   * @param jobId
   * @param jobResult
   * @param resultsStore
   */
  public Job(JobId jobId, JobResult jobResult, JobResultsStore resultsStore) {
    this.jobId = jobId;
    this.attempts.addAll(jobResult.getAttemptsList());
    this.resultsStore = checkNotNull(resultsStore);
  }

  public JobId getJobId() {
    return jobId;
  }

  public JobAttempt getJobAttempt() {
    Preconditions.checkState(attempts.size() >=1, "There should be at least one attempt in Job");
    int lastAttempt = attempts.size() - 1;
    return attempts.get(lastAttempt);
  }

  void addAttempt(final JobAttempt jobAttempt) {
    attempts.add(jobAttempt);
  }

  public List<JobAttempt> getAttempts() {
    return Collections.unmodifiableList(attempts);
  }

  @Override
  public String toString() {
    final JobAttempt jobAttempt = getJobAttempt();
    return format("{JobId: %s, SQL: %s, Dataset: %s, DatasetVersion: %s}",
            jobId.getId(), jobAttempt.getInfo().getSql(),
            PathUtils.constructFullPath(jobAttempt.getInfo().getDatasetPathList()),
            jobAttempt.getInfo().getDatasetVersion()); //todo
  }

  public JobData getData(){
    Preconditions.checkState(data != null || resultsStore != null, "not available from deserialized Job");
    if (data != null) {
      return data;
    }

    return resultsStore.get(jobId);
  }

  void setData(JobData data){
    Preconditions.checkArgument(this.data == null);
    this.data = data;
  }

  public boolean hasResults() {
    return resultsStore != null && resultsStore.jobOutputDirectoryExists(jobId);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(jobId, attempts);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof Job) {
        Job other = (Job) obj;
        return Objects.equal(jobId, other.jobId) && Objects.equal(attempts, other.attempts);
      }
    }
    return false;
  }
}

