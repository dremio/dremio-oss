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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.JobsServiceTestUtils;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.job.proto.JobSubmission;
import com.google.common.collect.ImmutableList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Testing e2e user sessions and session ids. */
public class TestUserSessionId extends BaseTestServer {

  @BeforeClass
  public static void init() throws Exception {
    BaseTestServer.init();
    BaseTestServer.getPopulator().populateTestUsers();
    setSystemOption(UserSession.ENABLE_SESSION_IDS, true);
  }

  @AfterClass
  public static void cleanup() {
    resetSystemOption(UserSession.ENABLE_SESSION_IDS);
  }

  @Test
  public void testSimpleSession() {
    final String sql = "SELECT 1";
    final String sessionId1 =
        getJobSubmissionAfterJobCompletion(getRequestFromSql(sql)).getSessionId().getId();
    assertNotNull("Session id cannot be null", sessionId1);

    // If we create the request with the same session id, it will run in the same session
    final String sessionId2 =
        getJobSubmissionAfterJobCompletion(getRequestFromSqlAndSessionId(sql, sessionId1))
            .getSessionId()
            .getId();
    assertNotNull("Session id cannot be null", sessionId2);
    assertEquals(sessionId1, sessionId2);
  }

  @Test
  public void testSimpleMultipleSessions() {
    final String sql = "SELECT 1";
    final JobRequest jobRequest = getRequestFromSql(sql);

    final String sessionId1 = getJobSubmissionAfterJobCompletion(jobRequest).getSessionId().getId();
    assertNotNull("Session id cannot be null", sessionId1);

    // If we create the request without session id, it will create a new session
    final String sessionId2 = getJobSubmissionAfterJobCompletion(jobRequest).getSessionId().getId();
    assertNotNull("Session id cannot be null", sessionId2);
    assertNotEquals(sessionId1, sessionId2);
  }

  @Test
  public void testInvalidSessionId() {
    final String sessionId = "foo";
    try {
      final String sql = "SELECT 1";
      final JobRequest jobRequest = getRequestFromSqlAndSessionId(sql, sessionId);
      final LogicalPlanCaptureListener planCaptureListener = new LogicalPlanCaptureListener();
      JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
          l(LocalJobsService.class), jobRequest, planCaptureListener);
      fail("Session id must be invalid");
    } catch (UserException e) {
      assertEquals(UserBitShared.DremioPBError.ErrorType.SYSTEM, e.getErrorType());
      assertTrue(
          e.getMessage().contains(String.format("Session id %s expired/not found.", sessionId)));
    }
  }

  @Test
  public void testMultiSqlInSingleSession() {
    // Create a new session by running a query
    final String sampleQuery =
        "SELECT \"employee_id\", \"full_name\" FROM cp.\"employees_with_null.json\"";
    final String alterQuery = "ALTER SESSION SET planner.leaf_limit_enable = %s";
    JobRequest jobRequest = getRequestFromSql(String.format(alterQuery, "false"));

    // Capture the sessionId
    final String sessionId = runQuery(jobRequest).getJobSubmission().getSessionId().getId();

    // Run the query and assert that leaf limit is disabled
    jobRequest = getRequestFromSqlAndSessionId(sampleQuery, sessionId);
    assertFalse(runQuery(jobRequest).getPlan().contains("SampleRel"));

    // Enable leaf limit
    jobRequest = getRequestFromSqlAndSessionId(String.format(alterQuery, "true"), sessionId);
    runQuery(jobRequest);

    // Run the same query again and assert that leaf limit is enabled
    jobRequest = getRequestFromSqlAndSessionId(sampleQuery, sessionId);
    assertTrue(runQuery(jobRequest).getPlan().contains("SampleRel"));
  }

  private static JobRequest getRequestFromSql(String sql) {
    return JobRequest.newBuilder().setSqlQuery(new SqlQuery(sql, DEFAULT_USERNAME)).build();
  }

  private static JobRequest getRequestFromSqlAndSessionId(String sql, String sessionId) {
    return JobRequest.newBuilder()
        .setSqlQuery(new SqlQuery(sql, ImmutableList.of(), DEFAULT_USERNAME, null, sessionId))
        .build();
  }

  private static Pair runQuery(JobRequest jobRequest) {
    final JoinPlanningMultiJoinPlanCaptureListener planCaptureListener =
        new JoinPlanningMultiJoinPlanCaptureListener();
    final JobSubmission jobSubmission =
        JobsServiceTestUtils.getJobSubmissionAfterJobCompletion(
            l(LocalJobsService.class), jobRequest, planCaptureListener);
    return Pair.of(jobSubmission, planCaptureListener.getPlan());
  }

  private static class Pair {
    private final JobSubmission jobSubmission;
    private final String plan;

    Pair(JobSubmission jobSubmission, String plan) {
      this.jobSubmission = jobSubmission;
      this.plan = plan;
    }

    public static Pair of(JobSubmission jobId, String plan) {
      return new Pair(jobId, plan);
    }

    public JobSubmission getJobSubmission() {
      return jobSubmission;
    }

    public String getPlan() {
      return plan;
    }
  }
}
