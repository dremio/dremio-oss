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
package com.dremio.dac.util;

import static com.dremio.service.jobs.JobsConstant.BYTES;
import static com.dremio.service.jobs.JobsConstant.DEFAULT;
import static com.dremio.service.jobs.JobsConstant.DEFAULT_DATASET_TYPE;
import static com.dremio.service.jobs.JobsConstant.EMPTY_DATASET_FIELD;
import static com.dremio.service.jobs.JobsConstant.EXTERNAL_QUERY;
import static com.dremio.service.jobs.JobsConstant.GIGABYTES;
import static com.dremio.service.jobs.JobsConstant.KILOBYTES;
import static com.dremio.service.jobs.JobsConstant.MEGABYTES;
import static com.dremio.service.jobs.JobsConstant.METADATA;
import static com.dremio.service.jobs.JobsConstant.UNAVAILABLE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.dremio.dac.explore.DatasetTool;
import com.dremio.dac.model.job.JobCancellationInfo;
import com.dremio.dac.model.job.JobFailureInfo;
import com.dremio.dac.model.job.JobFailureType;
import com.dremio.dac.model.job.QueryError;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.accelerator.proto.AccelerationDetails;
import com.dremio.service.accelerator.proto.ReflectionRelationship;
import com.dremio.service.accelerator.proto.SubstitutionState;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.RequestType;
import com.dremio.service.job.proto.DataSet;
import com.dremio.service.job.proto.DurationDetails;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.google.common.base.Preconditions;

/**
 * Util file for common methods in JobsListing and JobInfoDetails APIs
 */
public class JobUtil {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(JobUtil.class);

  public static List<DataSet> getQueriedDatasets(JobInfo jobInfo, RequestType requestType) {
   return buildQueriedDatasets(jobInfo.getParentsList(), requestType, jobInfo.getDatasetPathList());
  }

  public static List<DataSet> buildQueriedDatasets(List<ParentDatasetInfo> parents, RequestType requestType, List<String> pathList) {
    List<DataSet> queriedDatasets = new ArrayList<>();
    if (parents != null && parents.size() > 0) {
      parents.stream().forEach(
        parent -> {
          String datasetName = DEFAULT;
          String datasetType = DEFAULT_DATASET_TYPE;
          String datasetPath;
          List<String> datasetPathList = parent.getDatasetPathList();
          datasetName = datasetPathList.get(datasetPathList.size() - 1);
          datasetPath = StringUtils.join(datasetPathList, ".");
          if (!queriedDatasets.stream().anyMatch(dataSet -> dataSet.getDatasetPath().equals(datasetPath))) {
            if (!parent.getDatasetPathList().contains(EXTERNAL_QUERY)) {
              try {
                datasetType = parent.getType().name();
              } catch (NullPointerException ex) {
                datasetType = com.dremio.service.namespace.dataset.proto.DatasetType.values()[0].toString();
              }
            }
            populateQueriedDataset(queriedDatasets, datasetName, datasetType, datasetPath, parent.getDatasetPathList());
          }
        }
      );
    } else if (isTruePath(pathList)) {
      String datasetName = pathList.get(pathList.size() - 1);
      String datasetPath = StringUtils.join(pathList, ".");
      String datasetType = EMPTY_DATASET_FIELD;
      populateQueriedDataset(queriedDatasets, datasetName, datasetType, datasetPath, pathList);
    } else {
      populateQueriedDataset(queriedDatasets, UNAVAILABLE, EMPTY_DATASET_FIELD, EMPTY_DATASET_FIELD, new ArrayList<>());
      switch (requestType) {
        case GET_CATALOGS:
        case GET_COLUMNS:
        case GET_SCHEMAS:
        case GET_TABLES:
          queriedDatasets.get(queriedDatasets.size() - 1).setDatasetName(METADATA);
        default:
      }
    }
    return queriedDatasets;
  }

  public static List<DurationDetails> buildDurationDetails(List<UserBitShared.AttemptEvent> attemptEvent) {
    List<DurationDetails> durationDetails = new ArrayList<>();
    final List<UserBitShared.AttemptEvent> events = new ArrayList<>(attemptEvent);
    Collections.sort(events, stateStartTime);
    for (int i = 0; i < events.size() && !isTerminal(events.get(i).getState()); i++) {
          long timeSpent=0;
          // Condition to check if job state is RUNNING
          if(i==events.size() -1) {
            timeSpent = System.currentTimeMillis() - events.get(i).getStartTime();
          } else {
            timeSpent = events.get(i + 1).getStartTime() - events.get(i).getStartTime();
          }
          durationDetails.add(new DurationDetails());
          durationDetails.get(durationDetails.size() - 1).setPhaseName(events.get(i).getState().toString());
          durationDetails.get(durationDetails.size() - 1).setPhaseDuration(String.valueOf(timeSpent));
          durationDetails.get(durationDetails.size() -1).setPhaseStartTime(String.valueOf(events.get(i).getStartTime()));
        }
    return durationDetails;
  }

  public static String getConvertedBytes(Long bytes) {
    String returnVal = "";
    int loopCount = 0;
    while (bytes >= 1024 && loopCount <= 3) {
      bytes = bytes / 1024;
      loopCount++;
    }
    switch (loopCount) {
      case 0:
        returnVal = bytes.toString() + BYTES;
        break;
      case 1:
        returnVal = bytes.toString() + KILOBYTES;
        break;
      case 2:
        returnVal = bytes.toString() + MEGABYTES;
        break;
      case 3:
        returnVal = bytes.toString() + GIGABYTES;
        break;
    }
    return returnVal;
  }

  public static String getDatasetType(String datasetType) {
    return datasetType.contains("PHYSICAL_DATASET") ? "PHYSICAL_DATASET" : datasetType;
  }

  public static boolean isSnowflakeAccelerated(AccelerationDetails details) {
    if (details == null || details.getReflectionRelationshipsList() == null) {
      return false;
    }
    boolean isSnowFlakeAccelerated = false;

    for (ReflectionRelationship relationship : details.getReflectionRelationshipsList()) {
      if (relationship.getState() == SubstitutionState.CHOSEN && relationship.getSnowflake()) {
        isSnowFlakeAccelerated = true;
        break;
      }
    }
    return isSnowFlakeAccelerated;
  }

  public static String extractDatasetConfigName(DatasetConfig datasetConfig) {
    return datasetConfig.getFullPathList().get(datasetConfig.getFullPathList().size() - 1);
  }

  public static long getTotalDuration(JobSummary summary, boolean isFinalState) {
    long finishTime = summary.getEndTime();
    long currentMillisecond = System.currentTimeMillis();
    if (!isFinalState) {
      finishTime = currentMillisecond;
    }
    return finishTime - summary.getStartTime();
  }

  public static long getTotalDuration(JobDetails jobDetails, int lastAttemptIndex) {
    long startTime = jobDetails.getAttempts(0).getInfo().getStartTime() != 0 ? jobDetails.getAttempts(0).getInfo().getStartTime() : 0;
    long finishTime = jobDetails.getAttempts(lastAttemptIndex).getInfo().getFinishTime() != 0 ? jobDetails.getAttempts(lastAttemptIndex).getInfo().getFinishTime() : 0;
    long currentMillisecond = System.currentTimeMillis();
    if (!jobDetails.getCompleted()) {
      finishTime = currentMillisecond;
    }
    return finishTime - startTime;
  }

  public static JobFailureInfo toJobFailureInfo(String jobFailureInfo, com.dremio.service.job.proto.JobFailureInfo detailedJobFailureInfo) {
    if (detailedJobFailureInfo == null) {
      return new JobFailureInfo(jobFailureInfo, JobFailureType.UNKNOWN, null);
    }

    final JobFailureType failureType;
    if (detailedJobFailureInfo.getType() == null) {
      failureType = JobFailureType.UNKNOWN;
    } else {
      switch (detailedJobFailureInfo.getType()) {
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
      for (com.dremio.service.job.proto.JobFailureInfo.Error error : detailedJobFailureInfo.getErrorsList()) {
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

  public static boolean isTruePath(List<String> datasetPathList) {
    if(
      datasetPathList != null
        && !datasetPathList.equals(DatasetTool.TMP_DATASET_PATH.toPathList())
        && !datasetPathList.isEmpty()
        && !datasetPathList.get(0).equals("UNKNOWN")){
      return true;
    }

    return false;
  }

  public static boolean isComplete(JobState state) {
    Preconditions.checkNotNull(state, "JobState must be set");

    switch(state){
      case CANCELLATION_REQUESTED:
      case ENQUEUED:
      case NOT_SUBMITTED:
      case RUNNING:
      case STARTING:
      case PLANNING:
      case PENDING:
      case METADATA_RETRIEVAL:
      case QUEUED:
      case ENGINE_START:
      case EXECUTION_PLANNING:
        return false;
      case CANCELED:
      case COMPLETED:
      case FAILED:
        return true;
      default:
        throw new UnsupportedOperationException();
    }
  }

  private static void populateQueriedDataset(List<DataSet> queriedDatasets, String datasetName, String datasetType, String datasetPath, List<String> datasetPathList) {
    queriedDatasets.add(new DataSet());
    queriedDatasets.get(queriedDatasets.size() - 1).setDatasetName(datasetName);
    queriedDatasets.get(queriedDatasets.size() - 1).setDatasetPath(datasetPath);
    queriedDatasets.get(queriedDatasets.size() - 1).setDatasetType(datasetType);
    queriedDatasets.get(queriedDatasets.size() - 1).setDatasetPathsList(datasetPathList);
  }

  private static Comparator<UserBitShared.AttemptEvent> stateStartTime = new Comparator<UserBitShared.AttemptEvent>() {
    public int compare(final UserBitShared.AttemptEvent a1, final UserBitShared.AttemptEvent a2) {
      return Long.compare(a1.getStartTime(), a2.getStartTime());
    }
  };

  private static boolean isTerminal(UserBitShared.AttemptEvent.State state) {
    return (state == UserBitShared.AttemptEvent.State.COMPLETED ||
      state == UserBitShared.AttemptEvent.State.CANCELED ||
      state == UserBitShared.AttemptEvent.State.FAILED);
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

    } catch (NullPointerException e) {
      return null;
    }
  }

  public static JobState computeJobState(JobState lastAttemptState, boolean isJobCompleted) {
    // this is to avoid showing failed state for intermediate attempts when the job is being retried.
    if (!isJobCompleted && lastAttemptState == JobState.FAILED) {
      logger.debug("Changing jobState to RUNNING from {} as the job is being reattempted", lastAttemptState);
      return JobState.RUNNING;
    }
    return lastAttemptState;
  }
}
