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

import java.util.Optional;

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ResourceSchedulingInfo;

/**
 * Helper methods for computing various metrics out of {@link JobAttempt}
 */
public class AttemptsHelper {

  /**
   * @return wait before planning in milliseconds
   */
  public static long getCommandPoolWaitTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }

    return Optional.ofNullable(jobInfo.getCommandPoolWaitMillis()).orElse(0L);
  }
  /**
   * @return computed enqueued duration in milliseconds,
   *         null if {@link ResourceSchedulingInfo} is missing from the {@link JobInfo}
   */
  public static long getEnqueuedTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }

    final ResourceSchedulingInfo schedulingInfo = jobInfo.getResourceSchedulingInfo();
    if (schedulingInfo == null) {
      return 0;
    }

    final Long schedulingStart = schedulingInfo.getResourceSchedulingStart();
    Long schedulingEnd = schedulingInfo.getResourceSchedulingEnd();
    if (schedulingStart == null || schedulingEnd == null) {
      return 0;
    }

    if (jobAttempt.getState() == JobState.ENQUEUED) {
      // job is still enqueued, compute enqueued time up to now
      schedulingEnd = System.currentTimeMillis();
    } else {
      // schedulingEnd should always be greater than or equal to schedulingStart
      schedulingEnd = Math.max(schedulingStart, schedulingEnd);
    }

    return schedulingEnd - schedulingStart;
  }

  public static long getPoolWaitTime(JobAttempt jobAttempt) {
    return Optional.ofNullable(jobAttempt.getInfo().getCommandPoolWaitMillis()).orElse(0L);
  }

  public static long getPlanningTime(JobAttempt jobAttempt) {
    long planningScheduling = Optional.ofNullable(jobAttempt.getDetails().getTimeSpentInPlanning()).orElse(0L);
    final long enqueuedTime = getEnqueuedTime(jobAttempt);

    // take into account the fact that JobDetails.timeSpentInPlanning will only be set after the query starts executing
    planningScheduling = Math.max(planningScheduling, enqueuedTime);

    return planningScheduling - enqueuedTime;
  }

  public static long getExecutionTime(JobAttempt jobAttempt) {
    final JobInfo jobInfo = jobAttempt.getInfo();
    if (jobInfo == null) {
      return 0;
    }
    final JobDetails jobDetails = jobAttempt.getDetails();
    if (jobDetails == null) {
      return 0;
    }

    final long startTime = Optional.ofNullable(jobInfo.getStartTime()).orElse(0L);
    // finishTime is null until the query is no longer running
    final long finishTime = Optional.ofNullable(jobInfo.getFinishTime()).orElse(startTime);
    final long planningScheduling = Optional.ofNullable(jobDetails.getTimeSpentInPlanning()).orElse(0L);

    return finishTime - startTime - planningScheduling;
  }

}
