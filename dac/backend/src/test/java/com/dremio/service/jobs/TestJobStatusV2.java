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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery.QueryIdCapturingListener;
import com.dremio.QueryTestUtil;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.test.SampleDataPopulator;
import com.dremio.datastore.DatastoreException;
import com.dremio.exec.maestro.QueryTrackerImpl;
import com.dremio.exec.maestro.ResourceTracker;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.AttemptEvent.State;
import com.dremio.exec.proto.UserBitShared.QueryResult.QueryState;
import com.dremio.exec.proto.UserBitShared.QueryType;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.ChronicleGrpc.ChronicleBlockingStub;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;
import com.dremio.service.job.proto.JobId;
import com.google.common.base.Preconditions;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;


/**
 * Tests for JobStatusV2
 */
public class TestJobStatusV2 extends BaseTestServer {
  protected static final String DEFAULT_USERNAME = SampleDataPopulator.DEFAULT_USER_NAME;
  private static String query;

  private static Server server;
  private static ManagedChannel channel;
  private static JobsServiceStub asyncStub;
  private static ChronicleBlockingStub chronicleStub;

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
    chronicleStub = ChronicleGrpc.newBlockingStub(channel);

    query = getFile("tpch_quoted.sql");
    LocalJobsService localJobsService = l(LocalJobsService.class);
    localJobsService.getLocalAbandonedJobsHandler().reschedule(100);
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


  private static class StateProgressListener implements ExternalStatusListener {

    private List<AttemptEvent.State> stateList;
    private Map<AttemptEvent.State, Integer> progressionOrder;
    private Map<State, Long> stateDurations;

    StateProgressListener() {
      progressionOrder = new HashMap<>();
      progressionOrder.put(State.INVALID_STATE, 0);
      progressionOrder.put(State.PENDING, 1);
      progressionOrder.put(State.METADATA_RETRIEVAL, 2);
      progressionOrder.put(State.PLANNING, 3);
      progressionOrder.put(State.QUEUED, 4);
      progressionOrder.put(State.ENGINE_START, 5);
      progressionOrder.put(State.EXECUTION_PLANNING, 6);
      progressionOrder.put(State.STARTING, 7);
      progressionOrder.put(State.RUNNING, 8);
      progressionOrder = Collections.unmodifiableMap(progressionOrder);
      stateList = new ArrayList<>();
      stateDurations = new HashMap<>();
    }

    @Override
    public void queryProgressed(JobSummary jobSummary) {
      if (jobSummary.getStateListCount() == 0) {
        return;
      }
      AttemptEvent.State newState = jobSummary.getStateList(jobSummary.getStateListCount()-1).getState();
      AttemptEvent.State prevState;
      if (stateList.size() > 0) {
        prevState = stateList.get(stateList.size()-1);
        Preconditions.checkArgument(!isTerminal(prevState), "The state is terminal already.");

        if (isTerminal(newState)) {
          // Terminal state can be reached from previous running state or newState is failed or cancelled.
          // Otherwise throw exception.
          if (!isFailed(newState) && !isCancelled(newState) && prevState != State.RUNNING) {
            throw new RuntimeException("Wrong query progression order.");
          }
        } else if (progressionOrder.get(newState) < progressionOrder.get(prevState)) {
            throw new RuntimeException("Wrong query progression order.");
        }
      }
      stateList.add(newState);
      if (newState == State.COMPLETED) {
        List<AttemptEvent> eventList = new ArrayList<>(jobSummary.getStateListList());
        toStateDurations(eventList);
      }
    }

    @Override
    public void queryCompleted(JobSummary jobSummary) {
      stateList.add(jobSummary.getStateList(jobSummary.getStateListCount()-1).getState());
    }

    private void toStateDurations(List<AttemptEvent> events) {
      events.sort(Comparator.comparingLong(AttemptEvent::getStartTime));

      long timeSpent;
      for (int i=0; i < events.size(); i++) {
        if (isTerminal(events.get(i).getState())) {
          break;
        }
        if (i == events.size()-1) {
          timeSpent = System.currentTimeMillis() - events.get(i).getStartTime();
        } else {
          timeSpent = events.get(i+1).getStartTime() - events.get(i).getStartTime();
        }
        long finalTimeSpent = timeSpent;
        stateDurations.compute(events.get(i).getState(), (k,v) -> (v==null) ? finalTimeSpent : v + finalTimeSpent);
      }
    }

    boolean isTerminal(AttemptEvent.State state) {
      return (state == State.COMPLETED || state == State.CANCELED || state == State.FAILED);
    }

    boolean isFailed(AttemptEvent.State state) {
      return state == State.FAILED;
    }

    boolean isCancelled(AttemptEvent.State state) {
      return state == State.CANCELED;
    }

    public List<State> getStateList() {
      return stateList;
    }

    long getDuration(AttemptEvent.State state) {
      if (!stateDurations.containsKey(state)) {
        return -1;
      }
      return stateDurations.get(state);
    }

    AttemptEvent.State getLatestState() {
      if (stateList.size() == 0) {
        return State.INVALID_STATE;
      }
      return stateList.get(stateList.size()-1);
    }
  }

  private void registerJobStatusListener(JobId jobId, ExternalStatusListener listener) {
    ExternalListenerAdapter adapter = new ExternalListenerAdapter(listener);
    // wait for the job to be saved in store
    while (true) {
      try {
        Thread.sleep(50);
        chronicleStub.getJobSummary(
          JobSummaryRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setUserName(DEFAULT_USERNAME)
            .build());
        break;
      } catch (Exception e) {
        // ignore
      }
    }
    asyncStub.subscribeToJobEvents(JobsProtoUtil.toBuf(jobId), adapter);
  }

  private void pauseAndResume(Class clazz, String descriptor, State state) throws Exception {
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
    StateProgressListener stateListener = new StateProgressListener();
    registerJobStatusListener(jobId, stateListener);

    // wait to reach the state to pause
    while (stateListener.getLatestState() != state) {
      Thread.sleep(10);
    }
    Thread.sleep(1000);
    // resume now.
    getRpcClient().resumeQuery(queryId);

    listener.await();

    UserBitShared.QueryResult.QueryState queryState;
    while ((queryState = capturingListener.getQueryState()) == null) {
      Thread.sleep(10);
    }

    assertEquals(QueryState.COMPLETED, queryState);
    assertEquals(State.COMPLETED, stateListener.stateList.get(stateListener.stateList.size()-1));
  }

  @Test
  public void testCompletedState() throws Exception {
    pauseAndResume(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE, State.PLANNING);
  }

  private void pauseResumeAndCheckDuration(Class clazz, String descriptor, State state) throws Exception {
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
    StateProgressListener stateListener = new StateProgressListener();
    registerJobStatusListener(jobId, stateListener);

    // wait to reach the state to pause
    while (stateListener.getLatestState() != state) {
      Thread.sleep(10);
    }
    Thread.sleep(500);
    // resume now.
    getRpcClient().resumeQuery(queryId);

    listener.await();

    UserBitShared.QueryResult.QueryState queryState;
    while ((queryState = capturingListener.getQueryState()) == null) {
      Thread.sleep(10);
    }

    assertEquals(QueryState.COMPLETED, queryState);
    assertEquals(State.COMPLETED, stateListener.stateList.get(stateListener.stateList.size()-1));
    assertTrue(stateListener.getDuration(state) >= 500L);
  }

  @Test
  public void testPendingStateDuration() throws Exception {
    pauseResumeAndCheckDuration(AttemptManager.class, AttemptManager.INJECTOR_PENDING_PAUSE, State.PENDING);
  }

  @Test
  public void testMetadataRetrievalStateDuration() throws Exception {
    pauseResumeAndCheckDuration(AttemptManager.class, AttemptManager.INJECTOR_METADATA_RETRIEVAL_PAUSE, State.METADATA_RETRIEVAL);
  }

  @Test
  public void testPlanningStateDuration() throws Exception {
    pauseResumeAndCheckDuration(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE, State.PLANNING);
  }

  @Test
  public void testQueuedStateDuration() throws Exception {
    pauseResumeAndCheckDuration(ResourceTracker.class, ResourceTracker.INJECTOR_QUEUED_PAUSE, State.QUEUED);
  }

  @Test
  public void testExecutionPlanningStateDuration() throws Exception {
    pauseResumeAndCheckDuration(QueryTrackerImpl.class, QueryTrackerImpl.INJECTOR_EXECUTION_PLANNING_PAUSE, State.EXECUTION_PLANNING);
  }

  @Test
  public void testStartingStateDuration() throws Exception {
    pauseResumeAndCheckDuration(QueryTrackerImpl.class, QueryTrackerImpl.INJECTOR_STARTING_PAUSE, State.STARTING);
  }

  private void pauseCancelAndResume(Class clazz, String descriptor, State state) throws Exception {
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
    StateProgressListener stateListener = new StateProgressListener();
    registerJobStatusListener(jobId, stateListener);

    // wait to reach the state to pause
    while (stateListener.getLatestState() != state) {
      Thread.sleep(10);
    }

    // cancel query
    getRpcClient().cancelQuery(queryId);

    // resume now.
    getRpcClient().resumeQuery(queryId);

    listener.await();

    UserBitShared.QueryResult.QueryState queryState;
    while ((queryState = capturingListener.getQueryState()) == null) {
      Thread.sleep(10);
    }

    assertEquals(UserBitShared.QueryResult.QueryState.CANCELED, queryState);
    assertEquals(State.CANCELED, stateListener.stateList.get(stateListener.stateList.size()-1));
  }

  @Test
  public void testCancelledState() throws Exception {
    pauseCancelAndResume(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE, State.PLANNING);
  }

  private void injectException(String controls, String expectedDesc) throws Exception {
    try {
      ControlsInjectionUtil.setControls(getRpcClient(), controls);
      QueryIdCapturingListener capturingListener = new QueryIdCapturingListener();
      final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
      QueryTestUtil.testWithListener(getRpcClient(), QueryType.SQL, query, listener);

      // wait till we have a queryId
      UserBitShared.QueryId queryId;
      while ((queryId = capturingListener.getQueryId()) == null) {
        Thread.sleep(10);
      }
      JobId jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());
      StateProgressListener stateListener = new StateProgressListener();
      registerJobStatusListener(jobId, new StateProgressListener());

      listener.await();

      UserBitShared.QueryResult.QueryState queryState;
      while ((queryState = capturingListener.getQueryState()) == null) {
        Thread.sleep(10);
      }
      assertEquals(QueryState.FAILED, queryState);
      assertEquals(State.FAILED, stateListener.stateList.get(stateListener.stateList.size()-1));
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(expectedDesc));
    }
  }

  @Test
  public void testFailedState() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_PLAN_ERROR,
        ForemanException.class)
      .build();

    injectException(controls, AttemptManager.INJECTOR_PLAN_ERROR);
  }

  private void validateFailedJob(JobSummary jobSummary) throws Exception {
    Assert.assertSame(jobSummary.getJobState(), JobState.FAILED);
    Assert.assertNotNull(jobSummary.getFailureInfo());
    Assert.assertTrue(jobSummary.getFailureInfo().contains("Query failed due to kvstore or network errors. Details and profile information for this job may be partial or missing."));
  }

  private void injectProfileException(String controls, String expectedDesc) throws Exception {
    JobId jobId = null;
    try {
      ControlsInjectionUtil.setControls(getRpcClient(), controls);
      QueryIdCapturingListener capturingListener = new QueryIdCapturingListener();
      final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
      QueryTestUtil.testWithListener(getRpcClient(), QueryType.SQL, query, listener);

      // wait till we have a queryId
      UserBitShared.QueryId queryId;
      while ((queryId = capturingListener.getQueryId()) == null) {
        Thread.sleep(10);
      }
      jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

      listener.await();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(expectedDesc));
    }

    Assert.assertNotNull(jobId);

    JobSummary jobSummary = chronicleStub.getJobSummary(
      JobSummaryRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setUserName(DEFAULT_USERNAME)
        .build());

    validateFailedJob(jobSummary);
  }

  @Test
  public void testTailProfileFailedState() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_TAIL_PROFLE_ERROR,
        RuntimeException.class)
      .build();

    injectProfileException(controls, AttemptManager.INJECTOR_TAIL_PROFLE_ERROR);
  }

  @Test
  public void testGetFullProfileFailedState() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_GET_FULL_PROFLE_ERROR,
        RuntimeException.class)
      .build();

    injectProfileException(controls, AttemptManager.INJECTOR_GET_FULL_PROFLE_ERROR);
  }

  private void injectAttemptCompletionException(String controls) throws Exception {
    JobId jobId = null;
    ControlsInjectionUtil.setControls(getRpcClient(), controls);
    QueryIdCapturingListener capturingListener = new QueryIdCapturingListener();
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
    QueryTestUtil.testWithListener(getRpcClient(), QueryType.SQL, query, listener);

    // wait till we have a queryId
    UserBitShared.QueryId queryId;
    while ((queryId = capturingListener.getQueryId()) == null) {
      Thread.sleep(10);
    }
    jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

    listener.await();

    Assert.assertNotNull(jobId);

    while (true) {
      JobSummary jobSummary = chronicleStub.getJobSummary(
        JobSummaryRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(jobId))
          .setUserName(DEFAULT_USERNAME)
          .build());
      if(jobSummary.getJobState() == JobState.FAILED) {
        validateFailedJob(jobSummary);
        break;
      }
      Thread.sleep(50);
    }
  }

  @Test
  public void testAttemptCompletionFailedState() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_ERROR,
        IOException.class)
      .build();

    injectAttemptCompletionException(controls);
  }

  @Test
  public void testAttemptCompletionKVFailedState() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_KV_ERROR,
        DatastoreException.class)
      .build();

    injectAttemptCompletionException(controls);
  }

  private void injectAttemptCompletionExceptionMulti(String controls) throws Exception {
    final String controls1 = Controls.newBuilder()
      .addPause(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE)
      .build();

    JobId jobId1 = null;
    ControlsInjectionUtil.setControls(getRpcClient(), controls1);
    QueryIdCapturingListener capturingListener1 = new QueryIdCapturingListener();
    final AwaitableUserResultsListener listener1 = new AwaitableUserResultsListener(capturingListener1);
    QueryTestUtil.testWithListener(getRpcClient(), QueryType.SQL, query, listener1);

    // wait till we have a queryId
    UserBitShared.QueryId queryId1;
    while ((queryId1 = capturingListener1.getQueryId()) == null) {
      Thread.sleep(10);
    }
    jobId1 = new JobId(new UUID(queryId1.getPart1(), queryId1.getPart2()).toString());

    while (true) {
      JobSummary jobSummary = chronicleStub.getJobSummary(
        JobSummaryRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(jobId1))
          .setUserName(DEFAULT_USERNAME)
          .build());
      JobState jobState = jobSummary.getJobState();
      // wait for the pause to be hit
      if (jobState == JobState.PLANNING) {
        break;
      }
      Thread.sleep(50);
    }

    Thread.sleep(1000);

    injectAttemptCompletionException(controls);

    // resume now.
    getRpcClient().resumeQuery(queryId1);

    listener1.await();

    UserBitShared.QueryResult.QueryState queryState;
    while ((queryState = capturingListener1.getQueryState()) == null) {
      Thread.sleep(10);
    }

    assertEquals(QueryState.COMPLETED, queryState);
    JobState jobState = chronicleStub.getJobSummary(
      JobSummaryRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId1))
        .setUserName(DEFAULT_USERNAME)
        .build()).getJobState();
    Assert.assertTrue(jobState == JobState.COMPLETED);
  }

  @Test
  public void testAttemptCompletionFailedStateMultipleJobs() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_ERROR,
        IOException.class)
      .build();

    injectAttemptCompletionExceptionMulti(controls);
  }
}
