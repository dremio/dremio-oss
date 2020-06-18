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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.dremio.exec.proto.beans.AttemptEvent;
import com.dremio.exec.proto.beans.AttemptEvent.State;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.google.common.base.Preconditions;

/**
 * Unit tests for {@link AttemptsHelper}
 * <br>
 * The following tests ensure that:<br>
 * - we compute sensible values for edge cases<br>
 * - ensure the assumptions we used to implement AttemptsHelper still hold (i.e. until planning is done JobDetails.timeSpentInPlanning is null
 */
public class TestAttemptsHelper {

  private AttemptsHelper attemptsHelper;

  public TestAttemptsHelper() {
    List<AttemptEvent> events = new ArrayList<>();
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.PENDING).setStartTime(1));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.METADATA_RETRIEVAL).setStartTime(2));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.PLANNING).setStartTime(3));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.ENGINE_START).setStartTime(4));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.QUEUED).setStartTime(5));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.EXECUTION_PLANNING).setStartTime(6));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.STARTING).setStartTime(7));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.RUNNING).setStartTime(8));
    events.add(new com.dremio.exec.proto.beans.AttemptEvent().setState(State.COMPLETED).setStartTime(9));

    JobAttempt jobAttempt = new JobAttempt().setStateListList(events);
    attemptsHelper = new AttemptsHelper(jobAttempt);
  }

  @Test
  public void testGetEnqueuedTime() {
    // job that has been enqueued for at 3 hours
    final ResourceSchedulingInfo schedulingInfo = new ResourceSchedulingInfo()
      .setResourceSchedulingStart(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3))
      .setResourceSchedulingEnd(0L);
    final JobInfo info = new JobInfo()
      .setResourceSchedulingInfo(schedulingInfo);
    final JobAttempt attempt = new JobAttempt()
      .setInfo(info)
      .setState(JobState.ENQUEUED);

    assertTrue("Enqueued job has wrong enqueued time",
      AttemptsHelper.getLegacyEnqueuedTime(attempt) >= TimeUnit.HOURS.toMillis(3));
  }

  @Test
  public void testGetEnqueuedTimeFailedJob() {
    // job that has been enqueued for at 3 hours
    final ResourceSchedulingInfo schedulingInfo = new ResourceSchedulingInfo()
      .setResourceSchedulingStart(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3))
      .setResourceSchedulingEnd(0L);
    final JobInfo info = new JobInfo()
      .setResourceSchedulingInfo(schedulingInfo);
    final JobAttempt attempt = new JobAttempt()
      .setInfo(info)
      .setState(JobState.FAILED);

    assertEquals(0L, AttemptsHelper.getLegacyEnqueuedTime(attempt));
  }

  @Test
  public void testGetPlanningTime() {
    // enqueued job
    final ResourceSchedulingInfo schedulingInfo = new ResourceSchedulingInfo()
      .setResourceSchedulingStart(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3))
      .setResourceSchedulingEnd(0L);
    final JobInfo info = new JobInfo()
      .setResourceSchedulingInfo(schedulingInfo);
    final JobAttempt attempt = new JobAttempt()
      .setInfo(info)
      .setState(JobState.ENQUEUED)
      .setDetails(new JobDetails());

    assertEquals(0L, AttemptsHelper.getLegacyPlanningTime(attempt));
  }

  @Test
  public void testGetExecutionTime() {
    // enqueued job
    final ResourceSchedulingInfo schedulingInfo = new ResourceSchedulingInfo()
      .setResourceSchedulingStart(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3))
      .setResourceSchedulingEnd(0L);
    final JobInfo info = new JobInfo()
      .setResourceSchedulingInfo(schedulingInfo);
    final JobAttempt attempt = new JobAttempt()
      .setInfo(info)
      .setState(JobState.ENQUEUED)
      .setDetails(new JobDetails());

    assertEquals(0L, AttemptsHelper.getLegacyExecutionTime(attempt));
  }

  @Test
  public void testGetPendingTime() {
    Preconditions.checkNotNull(attemptsHelper.getPendingTime());
    assertTrue(1L == attemptsHelper.getPendingTime());
  }
  @Test
  public void testGetMetadataRetrievalTime() {
    Preconditions.checkNotNull(attemptsHelper.getMetadataRetrievalTime());
    assertTrue(1L == attemptsHelper.getMetadataRetrievalTime());
  }

  @Test
  public void testPlanningTime() {
    Preconditions.checkNotNull(attemptsHelper.getPlanningTime());
    assertTrue(1L == attemptsHelper.getPlanningTime());
  }

  @Test
  public void testGetEngineStartTime() {
    Preconditions.checkNotNull(attemptsHelper.getEngineStartTime());
    assertTrue(1L == attemptsHelper.getEngineStartTime());
  }

  @Test
  public void testGetQueuedTime() {
    Preconditions.checkNotNull(attemptsHelper.getQueuedTime());
    assertTrue(1L == attemptsHelper.getQueuedTime());
  }

  @Test
  public void testGetExecutionPlanningTime() {
    Preconditions.checkNotNull(attemptsHelper.getExecutionPlanningTime());
    assertTrue(1L == attemptsHelper.getExecutionPlanningTime());
  }

  @Test
  public void testGetStartingTime() {
    Preconditions.checkNotNull(attemptsHelper.getStartingTime());
    assertTrue(1L == attemptsHelper.getStartingTime());
  }

  @Test
  public void testGetRunningTime() {
    Preconditions.checkNotNull(attemptsHelper.getRunningTime());
    assertTrue(1L == attemptsHelper.getRunningTime());
  }
}
