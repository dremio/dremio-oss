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

import static java.lang.Thread.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.google.common.collect.ImmutableList;

/**
 * Test for failure to submit a job in job service.
 */
public class TestJobSubmit extends BaseTestServer {

  private LocalJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (LocalJobsService) l(JobsService.class);
  }

  @Test
  public void testFailedToSubmitJob() throws Exception {

    SqlQuery query = getQueryFromSQL("alter system set \"planner.memory_limit\"=10485760000");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE)
    );

    query = getQueryFromSQL("alter system set \"planner.reservation_bytes\"=10485760000");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE)
    );

    query = getQueryFromSQL("SELECT 1");
    CompletableFuture<Job> job = jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE);

    while (!job.isDone()) {
      sleep(1);
    }

    assertTrue(job.isCompletedExceptionally());

    List<Job> jobs = ImmutableList.copyOf(jobsService.getAllJobs("jst==FAILED", null, null, 0, Integer.MAX_VALUE, DEFAULT_USERNAME));
    assertEquals(1, jobs.size());
  }
}
