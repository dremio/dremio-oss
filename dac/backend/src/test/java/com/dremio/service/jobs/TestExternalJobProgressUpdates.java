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

import static com.dremio.BaseTestQuery.getFile;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery.QueryIdCapturingListener;
import com.dremio.QueryTestUtil;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;
import com.dremio.service.job.proto.JobId;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;


/**
 * Tests for Job progress updates of External jobs (submitted through JDBC/ODBC clients)
 */
public class TestExternalJobProgressUpdates extends BaseTestServer {
  protected static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  private static String query;

  private static Server server;
  private static ManagedChannel channel;
  private static JobsServiceStub asyncStub;

  @BeforeClass
  public static void setUp() throws Exception {
    final String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name)
      .directExecutor()
      .addService(new JobsServiceAdapter(p(LocalJobsService.class)))
      .addService(new Chronicle(p(LocalJobsService.class)))
      .build();
    server.start();

    channel = InProcessChannelBuilder.forName(name)
      .directExecutor()
      .build();
    asyncStub = JobsServiceGrpc.newStub(channel);

    query = getFile("tpch_quoted.sql");
  }

  @AfterClass
  public static void cleanUp() {
    if (server != null) {
      server.shutdown();
    }
    if (channel != null) {
      channel.shutdown();
    }
  }

  private class StateProgressListener implements ExternalStatusListener {

    private List<JobState> stateList;

    StateProgressListener() {
      stateList = new ArrayList<>();
    }

    @Override
    public void queryProgressed(JobSummary jobSummary) {
      stateList.add(jobSummary.getJobState());
    }

    @Override
    public void queryCompleted(JobSummary jobSummary) {
      stateList.add(jobSummary.getJobState());
    }

    public List<JobState> getStateList() {
      return stateList;
    }

    public boolean isCompleted() {
      JobState state = stateList.get(stateList.size()-1);
      return (state == JobState.COMPLETED || state == JobState.CANCELED || state == JobState.FAILED);
    }
  }

  private void registerJobStatusListener(JobId jobId, ExternalStatusListener listener) {
    ExternalListenerAdapter adapter = new ExternalListenerAdapter(listener);
    asyncStub.subscribeToJobEvents(JobsProtoUtil.toBuf(jobId), adapter);
  }

  private Job getJob(JobId jobId) throws Exception {
    try {
      GetJobRequest getJobRequest = GetJobRequest.newBuilder()
        .setJobId(jobId)
        .setUserName(SYSTEM_USERNAME)
        .build();
      return l(LocalJobsService.class).getJob(getJobRequest);
    } catch (JobNotFoundException e) {
      return null;
    }
  }

  private void pauseAndResume(Class clazz, String descriptor) throws Exception {
    final String controls = Controls.newBuilder()
      .addPause(clazz, descriptor)
      .build();

    ControlsInjectionUtil.setControls(getRpcClient(), controls);
    QueryIdCapturingListener capturingListener = new QueryIdCapturingListener();
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
    QueryTestUtil.testWithListener(getRpcClient(), UserBitShared.QueryType.SQL, query, listener);

    // wait till we have a queryId
    UserBitShared.QueryId queryId;
    while ((queryId = capturingListener.getQueryId()) == null) {
      Thread.sleep(10);
    }
    JobId jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

    // TODO(DX-26192): Job was not found
    // wait till job available to avoid JobNotFoundException
    while (getJob(jobId) == null) {
      Thread.sleep(10);
    }

    StateProgressListener stateListener = new StateProgressListener();
    registerJobStatusListener(jobId, stateListener);

    // wait for the pause to be hit
    Thread.sleep(1000);
    // resume now.
    getRpcClient().resumeQuery(queryId);

    listener.await();

    UserBitShared.QueryResult.QueryState queryState;
    while ((queryState = capturingListener.getQueryState()) == null) {
      Thread.sleep(10);
    }

    while (!stateListener.isCompleted()) {
      Thread.sleep(10);
    }

    assertEquals(QueryState.COMPLETED, queryState);
    assertEquals(JobState.COMPLETED, stateListener.getStateList().get(stateListener.getStateList().size()-1));
    // checks progress updates for intermediate states
    assertTrue(stateListener.getStateList().size() > 2);
  }

  @Test
  public void checkProgressUpdates() throws Exception {
    pauseAndResume(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE);
  }
}
