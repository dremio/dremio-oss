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
package com.dremio.exec.maestro;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.BaseTestQuery;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.exec.work.foreman.ForemanException;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.resource.basic.BasicResourceConstants;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;

/**
 * Tests failure scenarios in maestro and AttemptManager.
 */
public class TestMaestroResiliency extends BaseTestQuery {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(TestMaestroResiliency.class);

  private static final int QUEUE_LIMIT = 2;
  private static final int NUM_NODES = 3;
  private static String query;

  @BeforeClass
  public static void setUp() throws Exception {
    updateTestCluster(NUM_NODES, null);
    setSessionOption(BasicResourceConstants.SMALL_QUEUE_SIZE.getOptionName(),
      Integer.toString(QUEUE_LIMIT));
    setSessionOption("planner.slice_target", "10");

    query = getFile("queries/tpch/01.sql");
  }

  @AfterClass
  public static void tearDown() {
    resetSessionOption("planner.slice_target");
    resetSessionOption(BasicResourceConstants.SMALL_QUEUE_SIZE.getOptionName());
  }

  // Test with 1 more than the queue limit. If the semaphore isn't released correctly,
  // this will cause the last query to block and the test will timeout.
  private void runMultiple(String controls, String expectedDesc) throws Exception {
    int numExceptions = 0;
    for (int i = 0; i < QUEUE_LIMIT + 1; ++i) {
      try {
        ControlsInjectionUtil.setControls(client, controls);
        runSQL(query);
      } catch (UserRemoteException e) {
        assertTrue(e.getMessage().contains(expectedDesc));
        ++numExceptions;
      }
    }
    assertEquals(QUEUE_LIMIT + 1, numExceptions);
    waitTillQueryCleanup();
  }

  private void waitTillQueryCleanup() throws InterruptedException {
    while (getForemenCount() > 0 || getMaestroTrackerCount() > 0) {
      logger.info("waiting for query cleanup");
      Thread.sleep(10);
    }
  }

  private long getForemenCount() {
    return nodes[0].getBindingProvider().provider(ForemenWorkManager.class).get().getActiveQueryCount();
  }

  private long getMaestroTrackerCount() {
    return nodes[0].getBindingProvider().provider(MaestroService.class).get().getActiveQueryCount();
  }

  @Test
  public void testFailureInAttemptManagerConstructor() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_CONSTRUCTOR_ERROR,
        RuntimeException.class)
      .build();

    runMultiple(controls, AttemptManager.INJECTOR_CONSTRUCTOR_ERROR);
  }

  @Test
  public void testFailureInAttemptManagerBegin() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_TRY_BEGINNING_ERROR,
        ForemanException.class)
      .build();

    runMultiple(controls, AttemptManager.INJECTOR_TRY_BEGINNING_ERROR);
  }

  @Test
  public void testFailureInAttemptManagerEnd() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_TRY_END_ERROR,
        ForemanException.class)
      .build();

    runMultiple(controls, AttemptManager.INJECTOR_TRY_END_ERROR);
  }

  @Test
  public void testFailureInAttemptManagerPlanning() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_PLAN_ERROR,
        ForemanException.class)
      .build();

    runMultiple(controls, AttemptManager.INJECTOR_PLAN_ERROR);
  }

  @Test
  public void testFailureInMaestroQueryBegin() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(MaestroServiceImpl.class, MaestroServiceImpl.INJECTOR_EXECUTE_QUERY_BEGIN_ERROR,
        ExecutionSetupException.class)
      .build();

    runMultiple(controls, MaestroServiceImpl.INJECTOR_EXECUTE_QUERY_BEGIN_ERROR);
  }

  @Test
  public void testFailureInMaestroQueryEnd() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(MaestroServiceImpl.class, MaestroServiceImpl.INJECTOR_EXECUTE_QUERY_END_ERROR,
        ExecutionSetupException.class)
      .build();

    runMultiple(controls, MaestroServiceImpl.INJECTOR_EXECUTE_QUERY_END_ERROR);
  }

  @Test
  public void testFailureInMaestroCommandPoolSubmit() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(MaestroServiceImpl.class, MaestroServiceImpl.INJECTOR_COMMAND_POOL_SUBMIT_ERROR,
        ExecutionSetupException.class)
      .build();

    runMultiple(controls, MaestroServiceImpl.INJECTOR_COMMAND_POOL_SUBMIT_ERROR);
  }

  @Test
  public void testFailureInResourceAllocation() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(ResourceTracker.class, ResourceTracker.INJECTOR_RESOURCE_ALLOCATE_ERROR,
        IllegalStateException.class)
      .build();

    runMultiple(controls, ResourceTracker.INJECTOR_RESOURCE_ALLOCATE_ERROR);
  }

  // Same as testFailureInResourceAllocation, but different exception.
  @Test
  public void testUnavailableFailureInResourceAllocation() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(ResourceTracker.class, ResourceTracker.INJECTOR_RESOURCE_ALLOCATE_UNAVAILABLE_ERROR,
        ResourceUnavailableException.class)
      .build();

    runMultiple(controls, ResourceTracker.INJECTOR_RESOURCE_ALLOCATE_UNAVAILABLE_ERROR);
  }

  @Test
  public void testFailureInExecutionPlanning() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(QueryTrackerImpl.class, QueryTrackerImpl.INJECTOR_EXECUTION_PLANNING_ERROR,
        ExecutionSetupException.class)
      .build();

    runMultiple(controls, QueryTrackerImpl.INJECTOR_EXECUTION_PLANNING_ERROR);
  }

  @Test
  public void testFailureBeforeStartFragments() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(FragmentStarter.class,
        FragmentStarter.INJECTOR_BEFORE_START_FRAGMENTS_ERROR,
        IllegalStateException.class)
      .build();

    runMultiple(controls, FragmentStarter.INJECTOR_BEFORE_START_FRAGMENTS_ERROR);
  }

  @Test
  public void testFailureAfterStartFragments() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(FragmentStarter.class,
        FragmentStarter.INJECTOR_AFTER_START_FRAGMENTS_ERROR,
        IllegalStateException.class)
      .build();

    runMultiple(controls, FragmentStarter.INJECTOR_AFTER_START_FRAGMENTS_ERROR);
  }

  @Test
  public void testFailureBeforeActivateFragments() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(FragmentStarter.class,
        FragmentStarter.INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_ERROR,
        IllegalStateException.class)
      .build();

    runMultiple(controls, FragmentStarter.INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_ERROR);
  }

  @Test
  public void testFailureAfterActivateFragments() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(FragmentStarter.class,
        FragmentStarter.INJECTOR_AFTER_ACTIVATE_FRAGMENTS_ERROR,
        IllegalStateException.class)
      .build();

    runMultiple(controls, FragmentStarter.INJECTOR_AFTER_ACTIVATE_FRAGMENTS_ERROR);
  }

  @Test
  public void testFailureInExecutor() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(VectorizedHashAggOperator.class, VectorizedHashAggOperator.INJECTOR_SETUP_OOM_ERROR,
        OutOfMemoryException.class)
      .build();

    runMultiple(controls, VectorizedHashAggOperator.INJECTOR_SETUP_OOM_ERROR);
  }

  // the msg should be retried, and should succeed on retry.
  @Test
  public void testFailureInNodeCompletion() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(QueryTrackerImpl.class, QueryTrackerImpl.INJECTOR_NODE_COMPLETION_ERROR,
        RpcException.class)
      .build();

    for (int i = 0; i < QUEUE_LIMIT + 1; ++i) {
      ControlsInjectionUtil.setControls(client, controls);
      runSQL(query);
    }
    waitTillQueryCleanup();
  }

  // pause the test using execution controls, cancel and resume it.
  void pauseCancelAndResume(Class clazz, String descriptor) throws Exception {
    final String controls = Controls.newBuilder()
      .addPause(clazz, descriptor)
      .build();

    ControlsInjectionUtil.setControls(client, controls);
    QueryIdCapturingListener capturingListener = new QueryIdCapturingListener();
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
    testWithListener(UserBitShared.QueryType.SQL, query, listener);

    // wait till we have a queryId
    UserBitShared.QueryId queryId;
    while ((queryId = capturingListener.getQueryId()) == null) {
      Thread.sleep(10);
    }
    // wait for the pause to be hit
    Thread.sleep(3000);

    // cancel query
    client.cancelQuery(queryId);

    // resume now.
    client.resumeQuery(queryId);

    listener.await();

    UserBitShared.QueryResult.QueryState queryState = null;
    while ((queryState = capturingListener.getQueryState()) == null) {
      Thread.sleep(10);
    }
    assertEquals(UserBitShared.QueryResult.QueryState.CANCELED, queryState);

    waitTillQueryCleanup();
  }

  @Test
  public void testCancelDuringPlanning() throws Exception {
    pauseCancelAndResume(AttemptManager.class, AttemptManager.INJECTOR_PLAN_PAUSE);
  }

  @Test
  public void testCancelDuringResourceAllocation() throws Exception {
    pauseCancelAndResume(ResourceTracker.class, ResourceTracker.INJECTOR_RESOURCE_ALLOCATE_PAUSE);
  }

  @Test
  public void testCancelDuringExecutionPlanning() throws Exception {
    pauseCancelAndResume(QueryTrackerImpl.class, QueryTrackerImpl.INJECTOR_EXECUTION_PLANNING_PAUSE);
  }

  @Test
  public void testCancelDuringFragmentStart() throws Exception {
    pauseCancelAndResume(FragmentStarter.class, FragmentStarter.INJECTOR_BEFORE_START_FRAGMENTS_PAUSE);
  }

  @Test
  public void testCancelDuringFragmentActivate() throws Exception {
    pauseCancelAndResume(FragmentStarter.class, FragmentStarter.INJECTOR_BEFORE_ACTIVATE_FRAGMENTS_PAUSE);
  }
}
