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

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.dac.explore.model.CreateFromSQL;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.proto.JobState;

/**
 * SQLResource tests
 */
public class TestSQLResource extends BaseTestServer {
  private static final String PATH = "/sql/";
  private static final String JOB_PATH = "/job/";

  @Rule
  public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

  @Test
  public void submitQuery() throws InterruptedException {
    CreateFromSQL create = new CreateFromSQL("select * from sys.version", Collections.<String>emptyList());
    SQLResource.QueryDetails details = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildPost(Entity.entity(create, JSON)), SQLResource.QueryDetails.class);

    Assert.assertNotNull(details.getId());

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId())).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", ensureJobIsRunningOrFinishedWith(JobState.COMPLETED, jobState));

      if (jobState == JobState.COMPLETED) {
        Assert.assertNotNull(status.getRowCount());

        JobResourceData.JobDataResults data = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId()).path("results")).buildGet(), JobResourceData.JobDataResults.class);
        Assert.assertEquals(data.getRowCount(), (long) status.getRowCount());

        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void submitBadQuery() throws InterruptedException {
    CreateFromSQL create = new CreateFromSQL("select * from sys.versio", Collections.<String>emptyList());
    SQLResource.QueryDetails details = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildPost(Entity.entity(create, JSON)), SQLResource.QueryDetails.class);

    Assert.assertNotNull(details.getId());

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId())).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to fail", ensureJobIsRunningOrFinishedWith(JobState.FAILED, jobState));

      if (jobState == JobState.FAILED) {
        Assert.assertNull(status.getRowCount());

        // fetching details from a failed query should return 400
        expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId()).path("results")).buildGet());

        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testQueryResultsLimit() throws InterruptedException {
    CreateFromSQL create = new CreateFromSQL("select * from sys.version", Collections.<String>emptyList());
    SQLResource.QueryDetails details = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildPost(Entity.entity(create, JSON)), SQLResource.QueryDetails.class);

    Assert.assertNotNull(details.getId());

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId())).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", ensureJobIsRunningOrFinishedWith(JobState.COMPLETED, jobState));

      if (jobState == JobState.COMPLETED) {
        expectStatus(Response.Status.BAD_REQUEST, getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId()).path("results").queryParam("limit", 1000)).buildGet());
        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testGrantQueryNotSupported() throws InterruptedException {
    CreateFromSQL create = new CreateFromSQL("GRANT SELECT ON system TO USER user1", Collections.<String>emptyList());
    SQLResource.QueryDetails details = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildPost(Entity.entity(create, JSON)), SQLResource.QueryDetails.class);

    Assert.assertNotNull(details.getId());

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId())).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", ensureJobIsRunningOrFinishedWith(JobState.FAILED, jobState));

      if (jobState == JobState.FAILED) {
        Assert.assertTrue(status.getErrorMessage().contains("Enterprise Edition"));

        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  @Test
  public void testRevokeQueryNotSupported() throws InterruptedException {
    CreateFromSQL create = new CreateFromSQL("REVOKE SELECT ON system FROM USER user1", Collections.<String>emptyList());
    SQLResource.QueryDetails details = expectSuccess(getBuilder(getPublicAPI(3).path(PATH)).buildPost(Entity.entity(create, JSON)), SQLResource.QueryDetails.class);

    Assert.assertNotNull(details.getId());

    while (true) {
      JobStatus status = expectSuccess(getBuilder(getPublicAPI(3).path(JOB_PATH).path(details.getId())).buildGet(), JobStatus.class);

      JobState jobState = status.getJobState();

      Assert.assertTrue("expected job to complete successfully", ensureJobIsRunningOrFinishedWith(JobState.FAILED, jobState));

      if (jobState == JobState.FAILED) {
        Assert.assertTrue(status.getErrorMessage().contains("Enterprise Edition"));

        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }

  private boolean ensureJobIsRunningOrFinishedWith(JobState expectedFinalState, JobState state) {
    if (expectedFinalState.equals(state)) {
      return true;
    }

    return !finalJobStates.contains(state);
  }
}
