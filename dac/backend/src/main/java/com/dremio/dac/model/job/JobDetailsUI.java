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

import static com.dremio.dac.model.job.acceleration.UiMapper.toUI;
import static com.dremio.service.accelerator.AccelerationDetailsUtils.deserialize;
import static com.google.common.collect.ImmutableList.copyOf;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.util.Util;

import com.dremio.dac.model.job.acceleration.AccelerationDetailsUI;
import com.dremio.dac.resource.JobResource;
import com.dremio.proto.model.attempts.RequestType;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.job.proto.FileSystemDatasetProfile;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.JobStats;
import com.dremio.service.job.proto.MaterializationSummary;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.job.proto.SpillJobDetails;
import com.dremio.service.job.proto.TableDatasetProfile;
import com.dremio.service.job.proto.TopOperation;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Job details sent to UI. Derived from {@link JobDetails} except <code>paginationUrl</code>
 */
public class JobDetailsUI {
  private final JobId jobId;
  private final boolean snowflakeAccelerated;
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
  private final JobFailureInfo failureInfo;
  private final JobCancellationInfo cancellationInfo;
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
  private final AccelerationDetailsUI acceleration;
  private final ResourceSchedulingUI resourceScheduling;
  private final boolean spilled;
  private final SpillJobDetailsUI spillDetails;

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
    @JsonProperty("snowflakeAccelerated") boolean snowflakeAccelerated,
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
    @JsonProperty("failureInfo") JobFailureInfo failureInfo,
    @JsonProperty("cancellationInfo") JobCancellationInfo cancellationInfo,
    @JsonProperty("sql") String sql,
    @JsonProperty("description") String description,
    @JsonProperty("stats") JobStats stats,
    @JsonProperty("datasetType") DatasetType datasetType,
    @JsonProperty("datasetVersion") String datasetVersion,
    @JsonProperty("resultsAvailable") Boolean resultsAvailable,
    @JsonProperty("materializationFor") MaterializationSummary materializationFor,
    @JsonProperty("acceleration") AccelerationDetailsUI acceleration,
    @JsonProperty("resourceScheduling") ResourceSchedulingUI resourceScheduling,
    @JsonProperty("spilled") boolean spilled,
    @JsonProperty("spillDetails") SpillJobDetailsUI spillDetails) {
    this.jobId = jobId;
    this.queryType = queryType;
    this.datasetPathList = datasetPathList;
    this.parentsList = (parentsList != null) ? parentsList.stream().distinct().collect(Collectors.toList()) : null;
    this.state = state;
    this.startTime = startTime;
    this.endTime = endTime;
    this.user = user;
    this.requestType = requestType;
    this.isAccelerated = isAccelerated;
    this.snowflakeAccelerated = snowflakeAccelerated;
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
    this.cancellationInfo = cancellationInfo;
    this.description = description;
    this.sql = sql;
    this.stats = stats;
    this.datasetType = datasetType;
    this.datasetVersion = datasetVersion;
    this.resultsAvailable = resultsAvailable;
    this.materializationFor = materializationFor;
    this.acceleration = acceleration;
    this.resourceScheduling = resourceScheduling;
    this.spilled = spilled;
    this.spillDetails = spillDetails;
  }

  public static JobDetailsUI of(com.dremio.service.job.JobDetails jobDetails) {
    final List<JobAttempt> attempts = jobDetails.getAttemptsList().stream()
      .map(JobsProtoUtil::toStuff)
      .collect(Collectors.toList());
    final JobAttempt lastJobAttempt = Util.last(attempts);
    final JobInfo jobInfo = lastJobAttempt.getInfo();

    final AccelerationDetails accelerationDetails = deserialize(Util.last(attempts).getAccelerationDetails());
    final JobId jobId = JobsProtoUtil.toStuff(jobDetails.getJobId());
    return new JobDetailsUI(
        jobId,
        lastJobAttempt.getDetails(),
        JobResource.getPaginationURL(jobId),
        attempts,
        JobResource.getDownloadURL(jobDetails),
        toJobFailureInfo(jobInfo.getFailureInfo(), jobInfo.getDetailedFailureInfo()),
        toJobCancellationInfo(lastJobAttempt.getState(), lastJobAttempt.getInfo().getCancellationInfo()),
        lastJobAttempt.getInfo().getDatasetVersion(),
        jobDetails.getHasResults(),
        accelerationDetails,
        jobInfo.getSpillJobDetails());
  }

  public static JobFailureInfo toJobFailureInfo(String jobFailureInfo, com.dremio.service.job.proto.JobFailureInfo detailedJobFailureInfo) {
    if (detailedJobFailureInfo == null) {
      return new JobFailureInfo(jobFailureInfo, JobFailureType.UNKNOWN, null);
    }

    final JobFailureType failureType;
    if (detailedJobFailureInfo.getType() == null) {
      failureType = JobFailureType.UNKNOWN;
    } else {
      switch(detailedJobFailureInfo.getType()) {
      case PARSE:
        failureType = JobFailureType.PARSE;
        break;

      case VALIDATION:
        failureType = JobFailureType.VALIDATION;
        break;

      case EXECUTION:
        failureType = JobFailureType.EXECUTION;
        break;

      default:
        failureType = JobFailureType.UNKNOWN;
      }
    }

    final List<QueryError> errors;
    if (detailedJobFailureInfo.getErrorsList() == null) {
      errors = null;
    } else {
      errors = new ArrayList<>();
      for(com.dremio.service.job.proto.JobFailureInfo.Error error: detailedJobFailureInfo.getErrorsList()) {
        errors.add(new QueryError(error.getMessage(), toRange(error)));
      }
    }

    return new JobFailureInfo(detailedJobFailureInfo.getMessage(), failureType, errors);
  }

  public static JobCancellationInfo toJobCancellationInfo(JobState jobState, com.dremio.service.job.proto.JobCancellationInfo jobCancellationInfo) {
    if (jobState != JobState.CANCELED) {
      return null;
    }

    return new JobCancellationInfo(jobCancellationInfo == null ?
      "Query was cancelled" : //backward compatibility
      jobCancellationInfo.getMessage());
  }

  public static boolean wasSnowflakeAccelerated(AccelerationDetails details) {
    if (details == null || details.getReflectionRelationshipsList() == null) {
      return false;
    }

    boolean wasSnowflake = false;

    for (ReflectionRelationship relationship : details.getReflectionRelationshipsList()) {
      if (relationship.getState() == SubstitutionState.CHOSEN && relationship.getSnowflake()) {
        wasSnowflake = true;
        break;
      }
    }

    return wasSnowflake;
  }

  private static QueryError.Range toRange(com.dremio.service.job.proto.JobFailureInfo.Error error) {
    try {
      int startLine = error.getStartLine();
      int startColumn = error.getStartColumn();
      int endLine = error.getEndLine();
      int endColumn = error.getEndColumn();

      // Providing the UI with the following convention:
      // Ranges are 1-based and inclusive.
      return new QueryError.Range(startLine, startColumn, endLine, endColumn);

    } catch(NullPointerException e) {
      return null;
    }
  }
  public JobDetailsUI(
    JobId jobId,
    JobDetails jobDetails,
    String paginationUrl,
    List<JobAttempt> attempts,
    String downloadUrl,
    JobFailureInfo failureInfo,
    JobCancellationInfo cancellationInfo,
    String datasetVersion,
    Boolean resultsAvailable,
    AccelerationDetails accelerationDetails,
    SpillJobDetails spillDetails) {
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
        Util.last(attempts).getInfo().getAcceleration() != null,
        JobDetailsUI.wasSnowflakeAccelerated(accelerationDetails),
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
        AttemptsUIHelper.fromAttempts(jobId, attempts),
        AttemptsUIHelper.constructSummary(attempts),
        downloadUrl,
        failureInfo,
        cancellationInfo,
        attempts.get(0).getInfo().getSql(),
        attempts.get(0).getInfo().getDescription(),
        Util.last(attempts).getStats(),
        DatasetType.VIRTUAL_DATASET, // TODO: return correct result. This is closest since only the ui submits queries and they are using virtual datasets...
        datasetVersion,
        resultsAvailable,
        Util.last(attempts).getInfo().getMaterializationFor(),
        toUI(accelerationDetails),
        toRUI(attempts.get(0).getInfo().getResourceSchedulingInfo()),
        spillDetails != null,
        toSpillUI(spillDetails));
  }

  public static ResourceSchedulingUI toRUI(ResourceSchedulingInfo resourceSchedulingInfo) {
    if (resourceSchedulingInfo == null) {
      return null;
    }
    return new ResourceSchedulingUI(
      resourceSchedulingInfo.getQueueId(),
      resourceSchedulingInfo.getQueueName(),
      resourceSchedulingInfo.getRuleId(),
      resourceSchedulingInfo.getRuleName(),
      resourceSchedulingInfo.getRuleContent());
  }

  private static SpillJobDetailsUI toSpillUI(final SpillJobDetails spillJobDetails) {
    if (spillJobDetails == null) {
      return null;
    }
    return new SpillJobDetailsUI(spillJobDetails.getTotalBytesSpilledByHashAgg() > 0,
                                 spillJobDetails.getTotalBytesSpilledBySort() > 0,
                                 spillJobDetails.getTotalBytesSpilledByHashAgg() + spillJobDetails.getTotalBytesSpilledBySort());
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

  public JobFailureInfo getFailureInfo() {
    return failureInfo;
  }

  public JobCancellationInfo getCancellationInfo() {
    return cancellationInfo;
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

  public AccelerationDetailsUI getAcceleration() {
    return acceleration;
  }

  public boolean isSnowflakeAccelerated() {
    return snowflakeAccelerated;
  }

  public ResourceSchedulingUI getResourceScheduling() {
    return resourceScheduling;
  }

  public SpillJobDetailsUI getSpillDetails() {
    return spillDetails;
  }

  public boolean isSpilled() {
    return spilled;
  }
}
