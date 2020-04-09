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
import static org.junit.Assert.fail;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SearchJobsRequest;
import com.google.common.collect.ImmutableList;

/**
 * Test for failure to submit a job in job service.
 */
public class TestJobSubmit extends BaseTestServer {

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
  }

  @Test
  public void testFailedToSubmitJob() {

    SqlQuery query = getQueryFromSQL("alter system set \"planner.memory_limit\"=10485760000");
    submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());

    query = getQueryFromSQL("alter system set \"planner.reservation_bytes\"=10485760000");
    submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());

    query = getQueryFromSQL("SELECT 1");
    try {
      submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());
      fail("job should fail");
    } catch (Exception e) {
      // expected
    }

    List<JobSummary> jobs = ImmutableList.copyOf(
        l(JobsService.class).searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==FAILED")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
  }
}
