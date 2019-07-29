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
package com.dremio.sabot.exec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.OutOfMemoryException;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentSet;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.rpc.ResponseSender;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators;
import com.dremio.sabot.exec.fragment.FragmentExecutor;
import com.dremio.sabot.exec.fragment.FragmentExecutorBuilder;
import com.dremio.sabot.task.AsyncTask;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.dremio.sabot.task.Task;
import com.dremio.sabot.task.TaskDescriptor;
import com.dremio.sabot.task.TaskPool;
import com.dremio.sabot.threads.AvailabilityCallback;
import com.dremio.sabot.threads.sharedres.SharedResourceType;

/**
 * Unit test for {@link FragmentExecutors}
 */
public class TestFragmentExecutors {

  private class TestAsyncTask implements AsyncTask {
    Task.State taskState = Task.State.RUNNABLE;
    boolean cancelRequested = false;

    @Override
    public void run() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void refreshState() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public Task.State getState() {
      return taskState;
    }

    @Override
    public SharedResourceType getFirstBlockedResource() {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void updateSleepDuration(long duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void updateBlockedOnDownstreamDuration(long duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void updateBlockedOnUpstreamDuration(long duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void addBlockedOnSharedResourceDuration(SharedResourceType resource, long duration) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setWakeupCallback(AvailabilityCallback callback) {
      throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void setTaskDescriptor(TaskDescriptor descriptor) {
      // Ignored
    }

    public void setTaskState(Task.State taskState) {
      this.taskState = taskState;
    }

    void requestCancel() {
      this.cancelRequested = true;
    }

    boolean isCancelRequested() {
      return cancelRequested;
    }
  }

  @SuppressWarnings("unchecked") // needed for the unchecked cast below
  private SchedulingGroup<AsyncTaskWrapper> mockSchedulingGroup() {
    return (SchedulingGroup<AsyncTaskWrapper>)mock(SchedulingGroup.class);
  }

  private static class TestState implements AutoCloseable {
    private final FragmentExecutors fragmentExecutors;
    private final CoordExecRPC.InitializeFragments initializeFragments;
    private final List<AsyncTaskWrapper> runningTasks;
    private final Runnable actionOnStart;
    private boolean started;

    TestState(final FragmentExecutors fragmentExecutors,
              final CoordExecRPC.InitializeFragments initializeFragments,
              List<AsyncTaskWrapper> runningTasks,
              Runnable actionOnStart) {
      this.fragmentExecutors = fragmentExecutors;
      this.initializeFragments = initializeFragments;
      this.runningTasks = runningTasks;
      this.actionOnStart = actionOnStart;
      this.started = false;
    }
    FragmentExecutors getFragmentExecutors() {
      return fragmentExecutors;
    }
    CoordExecRPC.InitializeFragments getInitializeFragments() {
      return initializeFragments;
    }
    List<AsyncTaskWrapper> getRunningTasks() {
      return runningTasks;
    }
    TestState start() {
      assertFalse(started);
      actionOnStart.run();
      started = true;
      return this;
    }

    @Override
    public void close() throws Exception {
      if (started) {
        fragmentExecutors.close();
      }
    }

  }

  private TestState makeFragmentExecutor(final long queryKey, final int numFragments, final boolean exceptionAtStartup) throws Exception {
    OptionManager mockOptionManager = mock(OptionManager.class);
    when(mockOptionManager.getOption(any(TypeValidators.LongValidator.class)))
      .thenReturn(500L);

    List<AsyncTaskWrapper> runningTasks = new ArrayList<>();
    TaskPool mockTaskPool = mock(TaskPool.class);
    doAnswer((Answer<Object>) invocation -> {
      AsyncTaskWrapper taskWrapper = invocation.getArgumentAt(0, AsyncTaskWrapper.class);
      runningTasks.add(taskWrapper);
      return null;
    }).when(mockTaskPool).execute(any());

    final FragmentExecutors fe = new FragmentExecutors(
      mock(ExecToCoordTunnelCreator.class),
      mock(FragmentWorkManager.ExitCallback.class),
      mockTaskPool,
      mockOptionManager);

    final UserBitShared.QueryId queryId = UserBitShared.QueryId
      .newBuilder()
      .setPart1(123)
      .setPart2(queryKey)
      .build();

    ExecProtos.FragmentHandle fragmentHandles[] = new ExecProtos.FragmentHandle[numFragments];
    // First fragment is 0:0; All other fragments are: 1:(i-1)
    // (i.e., major ID + minor ID == fragment index)

    CoordExecRPC.InitializeFragments.Builder initializeFragmentsBuilder = CoordExecRPC.InitializeFragments.newBuilder();
    PlanFragmentSet.Builder setBuilder = initializeFragmentsBuilder.getFragmentSetBuilder();

    for (int i = 0; i < numFragments; i++) {
      int majorId = (i == 0) ? 0 : 1;
      int minorId = (i == 0) ? 0 : i - 1;

      fragmentHandles[i] = ExecProtos.FragmentHandle
        .newBuilder()
        .setQueryId(queryId)
        .setMajorFragmentId(majorId)
        .setMinorFragmentId(minorId)
        .build();

      final CoordExecRPC.PlanFragmentMinor planFragmentMinor = CoordExecRPC.PlanFragmentMinor
        .newBuilder()
        .setMajorFragmentId(majorId)
        .setMinorFragmentId(minorId)
        .build();

      if (i <= 1) {
        final CoordExecRPC.PlanFragmentMajor planFragmentMajor =
            CoordExecRPC.PlanFragmentMajor.newBuilder().setHandle(fragmentHandles[i]).build();

        setBuilder.addMajor(planFragmentMajor);
      }

      setBuilder.addMinor(planFragmentMinor);
    }

    final CoordExecRPC.InitializeFragments initializeFragments = initializeFragmentsBuilder.build();

    final CoordinationProtos.NodeEndpoint nodeEndpoint = CoordinationProtos.NodeEndpoint
      .newBuilder()
      .build();

    final FragmentExecutorBuilder mockFragmentExecutorBuilder = mock(FragmentExecutorBuilder.class);
    doAnswer((Answer<Object>) invocation -> {
      QueryStarter queryStarter = invocation.getArgumentAt(2, QueryStarter.class);
      queryStarter.buildAndStartQuery(mock(QueryTicket.class));
      return null;
    }).when(mockFragmentExecutorBuilder).buildAndStartQuery(any(), any(), any());

    FragmentExecutor fragmentExecutors[] = new FragmentExecutor[numFragments];
    AsyncTask asyncTasks[] = new AsyncTask[numFragments];
    for (int i = 0; i < numFragments; i++) {
      fragmentExecutors[i] = mock(FragmentExecutor.class);
      TestAsyncTask testAsyncTask = new TestAsyncTask();
      asyncTasks[i] = testAsyncTask;
      when(fragmentExecutors[i].getHandle()).thenReturn(fragmentHandles[i]);
      when(fragmentExecutors[i].getSchedulingGroup()).thenReturn(mockSchedulingGroup());
      when(fragmentExecutors[i].asAsyncTask()).thenReturn(asyncTasks[i]);
      FragmentExecutor.FragmentExecutorListener listener = mock(FragmentExecutor.FragmentExecutorListener.class);
      doAnswer((Answer<Object>) invocation -> {
        testAsyncTask.requestCancel();
        return null;
      }).when(listener).cancel();
      when(fragmentExecutors[i].getListener()).thenReturn(listener);
    }

    doAnswer((Answer<FragmentExecutor>) invocation -> {
      if (exceptionAtStartup) {
        throw new OutOfMemoryException();
      }
      PlanFragmentFull planFragment = invocation.getArgumentAt(1, PlanFragmentFull.class);
      int majorId = planFragment.getMajorFragmentId();
      int minorId = planFragment.getMinorFragmentId();
      return fragmentExecutors[majorId + minorId];
    }).when(mockFragmentExecutorBuilder).build(any(), any(), any(), any(), any());

    return new TestState(fe, initializeFragments, runningTasks, new Runnable() {
      @Override
      public void run() {
        fe.startFragments(initializeFragments, mockFragmentExecutorBuilder, mock(ResponseSender.class), nodeEndpoint);
        for (int i = 0; i < initializeFragments.getFragmentSet().getMinorCount(); ++i) {
          FragmentHandle handle = getHandleForMinorFragment(initializeFragments, i);
          fe.activateFragment(handle);
        }
      }
    });
  }

  private FragmentHandle getHandleForMinorFragment(CoordExecRPC.InitializeFragments initializeFragments, int index) {
    PlanFragmentMajor major = initializeFragments.getFragmentSet().getMajor(index == 0 ? 0: 1);
    PlanFragmentMinor minor = initializeFragments.getFragmentSet().getMinor(index);
    PlanFragmentFull full = new PlanFragmentFull(major, minor);
    return full.getHandle();
  }

  // Single fragment in a query. Run to completion
  @Test
  public void testSingleFragComplete() throws Exception {
    final TestState testState = makeFragmentExecutor(1, 1, false)
      .start();

    // query was now 'started'. 'runningTasks' now contains a task for each of the running fragments
    assertEquals(1, testState.getRunningTasks().size());

    final FragmentExecutors fe = testState.getFragmentExecutors();
    assertEquals(1, fe.size());

    AsyncTaskWrapper frag = testState.getRunningTasks().get(0);
    frag.getCleaner().close();
    assertEquals(0, fe.size());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    FragmentHandle handle = getHandleForMinorFragment(initializeFragments, 0);
    ((FragmentHandler)fe.getEventProvider(handle)).testExpireNow();
    fe.getEvictionAction().run();
    assertEquals(0, fe.size());
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  // Multiple fragments in one query
  @Test
  public void testMultipleFragsComplete() throws Exception {
    final int numFrags = 3;
    final TestState testState = makeFragmentExecutor(2, numFrags, false)
      .start();

    // query was now 'started'. 'runningTasks' now contains a task for each of the running fragments
    assertEquals(numFrags, testState.getRunningTasks().size());

    final FragmentExecutors fe = testState.getFragmentExecutors();
    assertEquals(numFrags, fe.size());

    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    int numRunningFrags = numFrags;
    for (final AsyncTaskWrapper frag : testState.getRunningTasks()) {
      frag.getCleaner().close();
      --numRunningFrags;
      assertEquals(numRunningFrags, fe.size());
    }
    assertEquals(0, fe.size());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    for (int i = 0; i < numFrags; i++) {
      FragmentHandle handle = getHandleForMinorFragment(initializeFragments, i);
      ((FragmentHandler) fe.getEventProvider(handle)).testExpireNow();
    }
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  // Single fragment, cancelled before completion
  @Test
  public void testSingleFragCancelled() throws Exception {
    final TestState testState = makeFragmentExecutor(3, 1, false)
      .start();

    // query was now 'started'. 'runningTasks' now contains a task for each of the running fragments
    assertEquals(1, testState.getRunningTasks().size());

    final FragmentExecutors fe = testState.getFragmentExecutors();
    assertEquals(1, fe.size());

    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    final FragmentHandle fragment0Handle = getHandleForMinorFragment(initializeFragments, 0);
    fe.cancelFragment(fragment0Handle);
    for (final AsyncTaskWrapper frag : testState.getRunningTasks()) {
      final TestAsyncTask underlyingTask = (TestAsyncTask)frag.getAsyncTask();
      if (underlyingTask.isCancelRequested()) {
        frag.getCleaner().close();
      }
    }
    assertEquals(0, fe.size());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    ((FragmentHandler)fe.getEventProvider(fragment0Handle)).testExpireNow();
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  // Single fragment, cancelled before completion
  @Test
  public void testMultipleFragCancelled() throws Exception {
    final int numFragments = 3;
    final TestState testState = makeFragmentExecutor(4, numFragments, false)
      .start();

    // query was now 'started'. 'runningTasks' now contains a task for each of the running fragments
    assertEquals(numFragments, testState.getRunningTasks().size());

    final FragmentExecutors fe = testState.getFragmentExecutors();
    assertEquals(numFragments, fe.size());

    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    for (int i = 0; i < numFragments; i++) {
      final ExecProtos.FragmentHandle fragmentHandle = getHandleForMinorFragment(initializeFragments, i);
      fe.cancelFragment(fragmentHandle);
    }
    for (final AsyncTaskWrapper frag : testState.getRunningTasks()) {
      final TestAsyncTask underlyingTask = (TestAsyncTask)frag.getAsyncTask();
      if (underlyingTask.isCancelRequested()) {
        frag.getCleaner().close();
      }
    }
    assertEquals(0, fe.size());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    for (int i = 0; i < numFragments; i++) {
      FragmentHandle handle = getHandleForMinorFragment(initializeFragments, i);
      ((FragmentHandler) fe.getEventProvider(handle)).testExpireNow();
    }
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  // Single fragment, cancelled after completion
  @Test
  public void testSingleFragPostCancelled() throws Exception {
    final TestState testState = makeFragmentExecutor(5, 1, false)
      .start();

    // query was now 'started'. 'runningTasks' now contains a task for each of the running fragments
    assertEquals(1, testState.getRunningTasks().size());

    final FragmentExecutors fe = testState.getFragmentExecutors();
    assertEquals(1, fe.size());

    AsyncTaskWrapper frag = testState.getRunningTasks().get(0);
    frag.getCleaner().close();
    assertEquals(0, fe.size());
    assertEquals(1, fe.getNumHandlers());

    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    final FragmentHandle fragment0Handle = getHandleForMinorFragment(initializeFragments, 0);
    fe.cancelFragment(fragment0Handle);
    assertEquals(0, fe.size());
    assertEquals(1, fe.getNumHandlers());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    ((FragmentHandler)fe.getEventProvider(fragment0Handle)).testExpireNow();
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  // Single fragment, cancelled before creation
  @Test
  public void testSingleFragPreCancelled() throws Exception {
    final TestState testState = makeFragmentExecutor(6, 1, false);

    final FragmentExecutors fe = testState.getFragmentExecutors();

    // query not yet started
    assertEquals(0, testState.getRunningTasks().size());
    assertEquals(0, fe.getNumHandlers());

    // Cancel before start
    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    final FragmentHandle fragment0Handle = getHandleForMinorFragment(initializeFragments, 0);
    fe.cancelFragment(fragment0Handle);

    // Cancellation entered in the frag handlers
    assertEquals(0, fe.size());
    assertEquals(1, fe.getNumHandlers());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    ((FragmentHandler)fe.getEventProvider(fragment0Handle)).testExpireNow();
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());

    testState.close();
  }

  @Test
  public void testSingleFragCreationFailure() throws Exception {
    final TestState testState = makeFragmentExecutor(7, 1, true);

    final FragmentExecutors fe = testState.getFragmentExecutors();
    // query not yet started
    assertEquals(0, testState.getRunningTasks().size());
    assertEquals(0, fe.getNumHandlers());

    // Starting will throw an exception, which will be sent out as a failure on the FragmentExecutor's "response sender"
    testState.start();
    assertEquals(0, fe.size());
    assertEquals(0, testState.getRunningTasks().size());
    assertEquals(1, fe.getNumHandlers());

    // Eviction should get rid of all the fragments, once the time for expiration (artificially) arrives
    final CoordExecRPC.InitializeFragments initializeFragments = testState.getInitializeFragments();
    final FragmentHandle fragment0Handle = getHandleForMinorFragment(initializeFragments, 0);
    ((FragmentHandler)fe.getEventProvider(fragment0Handle)).testExpireNow();
    fe.getEvictionAction().run();
    assertEquals(0, fe.getNumHandlers());
    assertEquals(0, fe.size());

    testState.close();
  }
}
