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
package com.dremio.dac.api;

import static com.dremio.service.jobs.JobsServiceUtil.finalJobStates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
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
  public void testGetJobResultsWithLargeLimitShouldFail() throws InterruptedException {
    SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    final JobId jobId = submitAndWaitUntilSubmitted(
      JobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.REST).build()
    );

    final String id = jobId.getId();

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id)).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", ensureJobIsRunningOrFinishedWith(JobState.COMPLETED, jobState));

      if (jobState == JobState.COMPLETED) {
        expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("results").queryParam("limit", 1000)).buildGet());
        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testGetJobResultsAreCorrect() throws InterruptedException {
    final SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder().setSqlQuery(query).setQueryType(QueryType.REST).build()
    );

    final String id = jobId.getId();

    final Response response = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("results").queryParam("limit", 1)).buildGet());
    final String body = response.readEntity(String.class);

    assertTrue(body.contains("\"rowCount\":1"));
    assertTrue(body.contains("\"schema\":[{\"name\":\"version\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"commit_id\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"commit_message\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"commit_time\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"build_email\",\"type\":{\"name\":\"VARCHAR\"}},{\"name\":\"build_time\",\"type\":{\"name\":\"VARCHAR\"}}]"));
    assertTrue(body.contains("\"rows\":[{\"version\":\"" + DremioVersionInfo.getVersion() + "\""));
  }

  @Test
  public void testCancelJob() throws InterruptedException {
    JobsService jobs = l(JobsService.class);

    SqlQuery query = new SqlQuery("select * from sys.version", Collections.emptyList(), SystemUser.SYSTEM_USERNAME);

    final String id = submitAndWaitUntilSubmitted(
      JobRequest.newBuilder()
        .setSqlQuery(query)
        .setQueryType(QueryType.REST)
        .build()
    ).getId();

    expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id).path("cancel")).buildPost(null));

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(id)).buildGet(), JobStatus.class);
      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to cancel successfully",
        Arrays
          .asList(JobState.PLANNING, JobState.RUNNING, JobState.STARTING, JobState.CANCELED, JobState.PENDING,
            JobState.METADATA_RETRIEVAL, JobState.QUEUED, JobState.ENGINE_START, JobState.EXECUTION_PLANNING)
          .contains(jobState));

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

  private boolean ensureJobIsRunningOrFinishedWith(JobState expectedFinalState, JobState state) {
    if (expectedFinalState.equals(state)) {
      return true;
    }

    return !finalJobStates.contains(state);
  }
}
