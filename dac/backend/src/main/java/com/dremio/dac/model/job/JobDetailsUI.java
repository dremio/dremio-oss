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

import static com.google.common.collect.ImmutableList.copyOf;

import java.util.List;

import org.apache.calcite.util.Util;

import com.dremio.dac.resource.JobResource;
import com.dremio.exec.proto.beans.RequestType;
import com.dremio.service.job.proto.FileSystemDatasetProfile;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.TableDatasetProfile;
import com.dremio.service.job.proto.TopOperation;
import com.dremio.service.jobs.Job;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Job details sent to UI. Derived from {@link JobDetails} except <code>paginationUrl</code>
 */
public class JobDetailsUI {
  private final JobId jobId;
  private final Integer plansConsidered;
  private final Long timeSpentInPlanning;
  private final Long waitInClient;
  private final Long dataVolume;
  private final Long outputRecords;
  private final Long peakMemory;
  private final List<TableDatasetProfile> tableDatasetProfiles;
  private final List<FileSystemDatasetProfile> fsDatasetProfiles;
  private final List<TopOperation> topOperations;
  private final String paginationUrl;
  private final List<AttemptDetailsUI> attemptDetails;
  private final String attemptsSummary;
  private final String downloadUrl;
  private final String failureInfo;
  private final QueryType queryType;
  private final List<String> datasetPathList;
  private final List<ParentDatasetInfo> parentsList;
  private final JobState state;
  private final Long startTime;
  private final Long endTime;
  private final String user;
  private final RequestType requestType;
  private final String sql;
  private final String description;
  private final boolean isAccelerated;
  private final JobStats stats;
  private final DatasetType datasetType;
  private final String datasetVersion;
  private final Boolean resultsAvailable;
  private final MaterializationSummary materializationFor;


  @JsonCreator
  public JobDetailsUI(
      @JsonProperty("jobId") JobId jobId,
      @JsonProperty("queryType") QueryType queryType,
      @JsonProperty("datasetPathList") List<String> datasetPathList,
      @JsonProperty("parentsList") List<ParentDatasetInfo> parentsList,
      @JsonProperty("state") JobState state,
      @JsonProperty("startTime") Long startTime,
      @JsonProperty("endTime") Long endTime,
      @JsonProperty("user") String user,
      @JsonProperty("jobType") RequestType requestType,
      @JsonProperty("accelerated") boolean isAccelerated,
      @JsonProperty("plansConsidered") Integer plansConsidered,
      @JsonProperty("timeSpentInPlanning") Long timeSpentInPlanning,
      @JsonProperty("waitInClient") Long waitInClient,
      @JsonProperty("dataVolume") Long dataVolume,
      @JsonProperty("outputRecords") Long outputRecords,
      @JsonProperty("peakMemory") Long peakMemory,
      @JsonProperty("tableDatasetProfiles") List<TableDatasetProfile> tableDatasetProfiles,
      @JsonProperty("fsDatasetProfiles") List<FileSystemDatasetProfile> fsDatasetProfiles,
      @JsonProperty("topOperations") List<TopOperation> topOperations,
      @JsonProperty("paginationUrl") String paginationUrl,
      @JsonProperty("attemptDetails") List<AttemptDetailsUI> attemptDetails,
      @JsonProperty("attemptsSummary") String attemptsSummary,
      @JsonProperty("downloadUrl") String downloadUrl,
      @JsonProperty("failureInfo") String failureInfo,
      @JsonProperty("sql") String sql,
      @JsonProperty("description") String description,
      @JsonProperty("stats") JobStats stats,
      @JsonProperty("datasetType") DatasetType datasetType,
      @JsonProperty("datasetVersion") String datasetVersion,
      @JsonProperty("resultsAvailable") Boolean resultsAvailable,
      @JsonProperty("materializationFor") MaterializationSummary materializationFor) {
    this.jobId = jobId;
    this.queryType = queryType;
    this.datasetPathList = datasetPathList;
    this.parentsList = parentsList;
    this.state = state;
    this.startTime = startTime;
    this.endTime = endTime;
    this.user = user;
    this.requestType = requestType;
    this.isAccelerated = isAccelerated;
    this.plansConsidered = plansConsidered;
    this.timeSpentInPlanning = timeSpentInPlanning;
    this.waitInClient = waitInClient;
    this.dataVolume = dataVolume;
    this.outputRecords = outputRecords;
    this.peakMemory = peakMemory;
    this.tableDatasetProfiles = tableDatasetProfiles == null ? null : copyOf(tableDatasetProfiles);
    this.fsDatasetProfiles = fsDatasetProfiles == null ? null : copyOf(fsDatasetProfiles);
    this.topOperations = topOperations == null ? null : copyOf(topOperations);
    this.paginationUrl = paginationUrl;
    this.attemptDetails = attemptDetails;
    this.attemptsSummary = attemptsSummary;
    this.downloadUrl = downloadUrl;
    this.failureInfo = failureInfo;
    this.description = description;
    this.sql = sql;
    this.stats = stats;
    this.datasetType = datasetType;
    this.datasetVersion = datasetVersion;
    this.resultsAvailable = resultsAvailable;
    this.materializationFor = materializationFor;
  }

  public JobDetailsUI(Job job){
    this(
        job.getJobId(),
        job.getJobAttempt().getDetails(),
        JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job),
        job.getJobAttempt().getInfo().getFailureInfo(), job.getJobAttempt().getInfo().getDatasetVersion(),
        job.hasResults());
  }

  public JobDetailsUI(
      JobId jobId,
      JobDetails jobDetails,
      String paginationUrl,
      List<JobAttempt> attempts,
      String downloadUrl,
      String failureInfo,
      String datasetVersion,
      Boolean resultsAvailable
      ) {
    this(
        jobId,
        attempts.get(0).getInfo().getQueryType(),
        JobsUI.asTruePathOrNull(attempts.get(0).getInfo().getDatasetPathList()),
        attempts.get(0).getInfo().getParentsList(),
        Util.last(attempts).getState(), // consider the last attempt state as the final job state
        attempts.get(0).getInfo().getStartTime(),
        Util.last(attempts).getInfo().getFinishTime(), // consider the last attempt finish time as the job finish time
        attempts.get(0).getInfo().getUser(),
        attempts.get(0).getInfo().getRequestType(),
        attempts.get(0).getInfo().getAcceleration() != null,
        jobDetails.getPlansConsidered(),
        jobDetails.getTimeSpentInPlanning(),
        jobDetails.getWaitInClient(),
        jobDetails.getDataVolume(),
        jobDetails.getOutputRecords(),
        jobDetails.getPeakMemory(),
        jobDetails.getTableDatasetProfilesList(),
        jobDetails.getFsDatasetProfilesList(),
        jobDetails.getTopOperationsList(),
        paginationUrl,
        AttemptsHelper.fromAttempts(jobId, attempts),
        AttemptsHelper.constructSummary(attempts),
        downloadUrl,
        failureInfo,
        attempts.get(0).getInfo().getSql(),
        attempts.get(0).getInfo().getDescription(),
        attempts.get(0).getStats(), // TODO: I don't think this is correct for multiple attempt case. This shows up in "Details" tab in Job Details
        DatasetType.VIRTUAL_DATASET, // TODO: return correct result. This is closest since only the ui submits queries and they are using virtual datasets...
        datasetVersion,
        resultsAvailable,
        attempts.get(0).getInfo().getMaterializationFor()
        );


  }

  public JobId getJobId() {
    return jobId;
  }

  public Integer getPlansConsidered() {
    return plansConsidered;
  }

  public Long getTimeSpentInPlanning() {
    return timeSpentInPlanning;
  }

  public Long getWaitInClient() {
    return waitInClient;
  }

  public Long getDataVolume() {
    return dataVolume;
  }

  public Long getOutputRecords() {
    return outputRecords;
  }

  public Long getPeakMemory() {
    return peakMemory;
  }

  public List<TableDatasetProfile> getTableDatasetProfiles() {
    return tableDatasetProfiles;
  }

  public List<FileSystemDatasetProfile> getFsDatasetProfiles() {
    return fsDatasetProfiles;
  }

  public List<TopOperation> getTopOperations() {
    return topOperations;
  }

  public String getPaginationUrl() {
    return paginationUrl;
  }

  public List<AttemptDetailsUI> getAttemptDetails() {
    return attemptDetails;
  }

  public String getAttemptsSummary() {
    return attemptsSummary;
  }

  public String getDownloadUrl() {
    return downloadUrl;
  }

  public String getFailureInfo() {
    return failureInfo;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public List<String> getDatasetPathList() {
    return datasetPathList;
  }

  public List<ParentDatasetInfo> getParentsList() {
    return parentsList;
  }

  public JobState getState() {
    return state;
  }

  public Long getStartTime() {
    return startTime;
  }

  public Long getEndTime() {
    return endTime;
  }

  public String getUser() {
    return user;
  }

  public RequestType getRequestType() {
    return requestType;
  }

  public boolean isAccelerated() {
    return isAccelerated;
  }

  public String getSql() {
    return sql;
  }

  public String getDescription() {
    return description;
  }

  public JobStats getStats() {
    return stats;
  }

  public DatasetType getDatasetType() {
    return datasetType;
  }

  public String getDatasetVersion() {
    return datasetVersion;
  }

  public Boolean getResultsAvailable() {
    return resultsAvailable;
  }

  public MaterializationSummary getMaterializationFor() {
    return materializationFor;
  }

}
