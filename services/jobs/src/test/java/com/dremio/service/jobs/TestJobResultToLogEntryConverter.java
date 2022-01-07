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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.Test;

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobCancellationInfo;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;

/**
 * Tests {@link JobResultToLogEntryConverter}
 */
public class TestJobResultToLogEntryConverter {

  @Test
  public void testOutcomeReasonCancelled() {
    JobResultToLogEntryConverter converter = new JobResultToLogEntryConverter();
    Job job = mock(Job.class);
    JobAttempt jobAttempt = new JobAttempt();
    JobInfo jobInfo = new JobInfo();
    JobId jobId = new JobId();
    jobId.setId("testId");
    jobInfo.setSql("Select * from nyc.taxis");
    jobInfo.setStartTime(System.currentTimeMillis());
    jobInfo.setFinishTime(System.currentTimeMillis());
    jobInfo.setUser("dummyUser");

    final String cancelReason = "this is the cancel reason";
    JobCancellationInfo jobCancellationInfo = new JobCancellationInfo();
    jobCancellationInfo.setMessage(cancelReason);

    when(job.getJobId()).thenReturn(jobId);
    jobAttempt.setState(JobState.CANCELED);
    jobAttempt.setInfo(jobInfo);
    jobInfo.setCancellationInfo(jobCancellationInfo);
    when(job.getJobAttempt()).thenReturn(jobAttempt);

    assertEquals(cancelReason, converter.apply(job).getOutcomeReason());
  }

  @Test
  public void testOutcomeReasonFailed() {
    JobResultToLogEntryConverter converter = new JobResultToLogEntryConverter();
    Job job = mock(Job.class);
    JobAttempt jobAttempt = new JobAttempt();
    JobInfo jobInfo = new JobInfo();
    JobId jobId = new JobId();
    jobId.setId("testId");
    jobInfo.setSql("Select * from nyc.taxis");
    jobInfo.setStartTime(System.currentTimeMillis());
    jobInfo.setFinishTime(System.currentTimeMillis());
    jobInfo.setUser("dummyUser");

    final String failureReason = "this is the failure reason";

    when(job.getJobId()).thenReturn(jobId);
    jobAttempt.setState(JobState.FAILED);
    jobAttempt.setInfo(jobInfo);
    jobInfo.setFailureInfo(failureReason);
    when(job.getJobAttempt()).thenReturn(jobAttempt);

    assertEquals(failureReason, converter.apply(job).getOutcomeReason());
  }
}
