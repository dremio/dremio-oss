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
package com.dremio.service.jobs;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import com.dremio.common.utils.PathUtils;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.SessionId;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** Job represents details of a currently running or completed query on a dataset. */
public class Job {

  private final JobId jobId;
  private final SessionId sessionId;
  private final List<JobAttempt> attempts = new CopyOnWriteArrayList<>();
  private final JobResultsStore resultsStore;
  private volatile long recordCount;
  private volatile boolean isInternal;
  private QueryProfile profile;
  private boolean
      profileDetailsCapturedPostTermination; // indicates whether job profile details were fetched
  // after job completed/failed/cancelled

  private JobData data;

  /**
   * true when all attempts complete. This is necessary as we cant' just rely on the last attempt's
   * state in case the query reattempts
   */
  private boolean completed;

  public Job(JobId jobId, JobAttempt jobAttempt, SessionId sessionId) {
    this.jobId = jobId;
    this.sessionId = sessionId;
    this.resultsStore = null;
    this.completed = false;
    attempts.add(checkNotNull(jobAttempt, "jobAttempt is null"));
  }

  public Job(JobId jobId, JobResult jobResult) {
    this.jobId = jobId;
    this.sessionId = jobResult.getSessionId();
    this.resultsStore = null;
    this.completed = jobResult.getCompleted();
    attempts.addAll(jobResult.getAttemptsList());
    this.profileDetailsCapturedPostTermination =
        jobResult.getProfileDetailsCapturedPostTermination();
  }

  /**
   * Create an instance which loads the job results lazily.
   *
   * @param jobId
   * @param jobResult
   * @param resultsStore
   * @param sessionId
   */
  public Job(JobId jobId, JobResult jobResult, JobResultsStore resultsStore, SessionId sessionId) {
    this.jobId = jobId;
    this.sessionId = sessionId;
    this.attempts.addAll(jobResult.getAttemptsList());
    this.resultsStore = checkNotNull(resultsStore);
    this.completed = jobResult.getCompleted();
    this.profileDetailsCapturedPostTermination =
        jobResult.getProfileDetailsCapturedPostTermination();
  }

  void setRecordCount(long recordCount) {
    this.recordCount = recordCount;
  }

  long getRecordCount() {
    return this.recordCount;
  }

  public JobId getJobId() {
    return jobId;
  }

  public SessionId getSessionId() {
    return sessionId;
  }

  public JobAttempt getJobAttempt() {
    Preconditions.checkState(attempts.size() >= 1, "There should be at least one attempt in Job");
    int lastAttempt = attempts.size() - 1;
    return attempts.get(lastAttempt);
  }

  public QueryType getQueryType() {
    Preconditions.checkState(attempts.size() >= 1, "There should be at least one attempt in Job");
    return attempts.get(0).getInfo().getQueryType();
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
    final String sessionIdStr = sessionId == null ? null : sessionId.getId();
    return format(
        "{JobId: %s, SessionId: %s, SQL: %s, Dataset: %s, DatasetVersion: %s}",
        jobId.getId(),
        sessionIdStr,
        jobAttempt.getInfo().getSql(),
        PathUtils.constructFullPath(jobAttempt.getInfo().getDatasetPathList()),
        jobAttempt.getInfo().getDatasetVersion()); // todo
  }

  public JobData getData() {
    Preconditions.checkState(
        data != null || resultsStore != null, "not available from deserialized Job");
    if (data != null) {
      return data;
    }
    return resultsStore.get(getJobId(), getSessionId());
  }

  void setData(JobData data) {
    Preconditions.checkArgument(this.data == null);
    this.data = data;
  }

  public boolean isCompleted() {
    return completed;
  }

  void setCompleted(boolean completed) {
    this.completed = completed;
  }

  /**
   * Check if this Job has results. Job results may exist for FAILED jobs. Users should be aware and
   * check JobState if necessary.
   */
  public boolean hasResults() {
    return resultsStore != null && resultsStore.jobOutputDirectoryExists(jobId);
  }

  void setIsInternal(boolean isInternal) {
    this.isInternal = isInternal;
  }

  boolean isInternal() {
    return isInternal;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(jobId, attempts, completed, sessionId);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj != null) {
      if (obj instanceof Job) {
        Job other = (Job) obj;
        return Objects.equal(jobId, other.jobId)
            && Objects.equal(attempts, other.attempts)
            && Objects.equal(completed, other.completed)
            && Objects.equal(sessionId, other.sessionId);
      }
    }
    return false;
  }

  JobResult toJobResult(Job job) {
    JobResult jobResult =
        new JobResult().setCompleted(completed).setAttemptsList(job.getAttempts());
    if (sessionId != null) {
      jobResult.setSessionId(sessionId);
    }
    jobResult.setProfileDetailsCapturedPostTermination(job.profileDetailsCapturedPostTermination());
    return jobResult;
  }

  public void setProfile(QueryProfile profile) {
    this.profile = profile;
  }

  public QueryProfile getProfile() {
    return this.profile;
  }

  public void setProfileDetailsCapturedPostTermination(boolean value) {
    this.profileDetailsCapturedPostTermination = value;
  }

  public boolean profileDetailsCapturedPostTermination() {
    return this.profileDetailsCapturedPostTermination;
  }
}
