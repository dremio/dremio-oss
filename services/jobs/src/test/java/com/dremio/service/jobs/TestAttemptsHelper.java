/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ResourceSchedulingInfo;

/**
 * Unit tests for {@link AttemptsHelper}
 * <br>
 * The following tests ensure that:<br>
 * - we compute sensible values for edge cases<br>
 * - ensure the assumptions we used to implement AttemptsHelper still hold (i.e. until planning is done JobDetails.timeSpentInPlanning is null
 */
public class TestAttemptsHelper {

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
      AttemptsHelper.getEnqueuedTime(attempt) >= TimeUnit.HOURS.toMillis(3));
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

    assertEquals(0L, AttemptsHelper.getEnqueuedTime(attempt));
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

    assertEquals(0L, AttemptsHelper.getPlanningTime(attempt));
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

    assertEquals(0L, AttemptsHelper.getExecutionTime(attempt));
  }

}
