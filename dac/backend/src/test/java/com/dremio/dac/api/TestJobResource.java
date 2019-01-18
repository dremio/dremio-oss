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
package com.dremio.dac.api;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.users.SystemUser;

/**
 * Jobs API tests
 */
public class TestJobResource extends BaseTestServer {
  private static final String JOB_PATH = "/job/";

  @Rule
  public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
  }

  @Test
  public void testGetJobResults() throws InterruptedException {
    JobsService jobs = l(JobsService.class);

    SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    Job job = jobs.submitJob(JobRequest.newBuilder()
      .setSqlQuery(query)
      .setQueryType(QueryType.REST)
      .build(), NoOpJobStatusListener.INSTANCE);


    String id = job.getJobId().getId();

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id)).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", Arrays.asList(JobState.COMPLETED, JobState.RUNNING, JobState.ENQUEUED, JobState.STARTING).contains(jobState));

      if (jobState == JobState.COMPLETED) {
        expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("results").queryParam("limit", 1000)).buildGet());
        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testCancelJob() throws InterruptedException {
    JobsService jobs = l(JobsService.class);

    SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    Job job = jobs.submitJob(JobRequest.newBuilder()
      .setSqlQuery(query)
      .setQueryType(QueryType.REST)
      .build(), NoOpJobStatusListener.INSTANCE);

    String id = job.getJobId().getId();

    expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("cancel")).buildPost(null));

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id)).buildGet(), JobStatus.class);
      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to cancel successfully", Arrays.asList(JobState.RUNNING, JobState.ENQUEUED, JobState.STARTING, JobState.CANCELLATION_REQUESTED, JobState.CANCELED).contains(jobState));

      if (jobState == JobState.CANCELED) {
        expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("results").queryParam("limit", 1000)).buildGet());
        assertEquals("Query cancelled by user 'dremio'", status.getCancellationReason());
        break;
      } else if (jobState == JobState.COMPLETED) {
        // the job could complete before the cancel request, so make sure the test doesn't fail in that case.
        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testBadJobId() throws Exception {
    expectStatus(Response.Status.NOT_FOUND, getBuilder(getPublicAPI(3).path(JOB_PATH).path("bad-id")).buildGet());
  }
}
