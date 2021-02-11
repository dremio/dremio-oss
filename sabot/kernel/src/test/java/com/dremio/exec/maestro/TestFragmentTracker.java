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

import static java.lang.Thread.sleep;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.common.concurrent.CloseableSchedulerThreadPool;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.util.Retryer;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataStatsCollector;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.work.foreman.CompletionListener;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.options.OptionList;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.dremio.service.executor.ExecutorServiceClient;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class TestFragmentTracker {
  private final QueryId queryId = QueryId
    .newBuilder()
    .setPart1(123L)
    .setPart2(456L)
    .build();

  private final NodeEndpoint selfEndpoint =
    NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();

  private CloseableSchedulerThreadPool closeableSchedulerThreadPool;

  @Mock
  private QueryContext context;

  @Mock
  private CompletionListener completionListener;

  @Mock
  private AttemptObserver observer;

  @Mock
  private Catalog catalog;

  @Mock
  private ClusterCoordinator coordinator;

  @Mock
  private OptionManager optionManager;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    this.closeableSchedulerThreadPool = new CloseableSchedulerThreadPool("cancel-fragment-retry-",1);

    // Boilerplate
    when(context.getQueryUserName()).thenReturn("myuser");
    when(context.getSession()).thenReturn(UserSession.Builder.newBuilder().build());
    when(context.getNonDefaultOptions()).thenReturn(new OptionList());
    when(context.getConfig()).thenReturn(SabotConfig.create());
    when(catalog.getMetadataStatsCollector()).thenReturn(new MetadataStatsCollector());

    when(optionManager.getOption(eq(ExecutorSetService.DREMIO_VERSION_CHECK))).thenReturn(true);

    when(coordinator.getServiceSet(Role.EXECUTOR)).thenReturn(new ServiceSet() {
      @Override
      public RegistrationHandle register(NodeEndpoint endpoint) {
        return null;
      }

      @Override
      public Collection<NodeEndpoint> getAvailableEndpoints() {
        return Collections.singletonList(selfEndpoint);
      }

      @Override
      public void addNodeStatusListener(NodeStatusListener listener) {

      }

      @Override
      public void removeNodeStatusListener(NodeStatusListener listener) {

      }
    });
  }

  @After
  public void tearDown() throws Exception {
    closeableSchedulerThreadPool.close();
  }

  /**
   * Check that retrier calls only the maximum number of times initialised and
   * if it succeeds before maximum number of retries, it should stop
   */
  @Test
  public void testCancelFragmentsHelper() throws Exception {
    FragmentTracker fragmentTracker = new FragmentTracker(queryId, completionListener, queryCloser, null,
      new LocalExecutorSetService(DirectProvider.wrap(coordinator),
                                  DirectProvider.wrap(optionManager)),
      closeableSchedulerThreadPool);

    ExecutorServiceClient mockExecutorServiceClient = Mockito.mock(ExecutorServiceClient.class);
    doNothing().when(mockExecutorServiceClient).cancelFragments(any(), any());

    ExecutorServiceClientFactory executorServiceClientFactory = Mockito.mock(ExecutorServiceClientFactory.class);
    doReturn(mockExecutorServiceClient).when(executorServiceClientFactory).getClientForEndpoint(any());

    Set<NodeEndpoint> pendingNodes = new HashSet<>();
    pendingNodes.add(NodeEndpoint.newBuilder().setAddress("host").setFabricPort(0).build());

    int MAX_RETRIES = 4;
    int baseMillis = 100;
    Retryer retryer = new Retryer.Builder()
      .setWaitStrategy(Retryer.WaitStrategy.FLAT, baseMillis, baseMillis)
      .retryIfExceptionOfType(StatusRuntimeException.class)
      .setMaxRetries(MAX_RETRIES)
      .build();

    FragmentTracker.SignalListener errorResponse = Mockito.mock(FragmentTracker.SignalListener.class);
    doReturn(new StatusRuntimeException(Status.INTERNAL)).when(errorResponse).getException();

    FragmentTracker.SignalListener successResponse = Mockito.mock(FragmentTracker.SignalListener.class);
    doReturn(null).when(successResponse).getException();

    fragmentTracker = spy(fragmentTracker);

    // Fail for timesFailures times and next time - return success
    int timesFailures = 2;
    int timesSuccess = 1;
    doReturn(errorResponse)
      .doReturn(errorResponse)
      .doReturn(successResponse)
      .when(fragmentTracker)
      .getResponseObserver(any(), any());

    fragmentTracker.cancelExecutingFragmentsInternalHelper(queryId, pendingNodes, executorServiceClientFactory, retryer, closeableSchedulerThreadPool);

    sleep(1000 + baseMillis * (timesFailures + timesSuccess)); // plus 1000 is done to have extra sleep.

    //To verify retry stops after success, when cancelFragments fails for timesFailures and then succeeds.
    Mockito.verify(mockExecutorServiceClient, Mockito.times(timesFailures + timesSuccess))
      .cancelFragments(Mockito.any(), Mockito.any());

    reset(fragmentTracker, mockExecutorServiceClient);
    doNothing().when(mockExecutorServiceClient).cancelFragments(any(), any());

    //To verify retry happened not more than MAX_RETRIES, when cancelFragments always fails.
    timesFailures = MAX_RETRIES;
    timesSuccess = 0;
    doReturn(errorResponse) // always return errorResponse
      .when(fragmentTracker)
      .getResponseObserver(any(), any());

    fragmentTracker.cancelExecutingFragmentsInternalHelper(queryId, pendingNodes, executorServiceClientFactory, retryer, closeableSchedulerThreadPool);
    sleep(1000 + baseMillis * (timesFailures + timesSuccess)); // plus 1000 is done to have extra sleep.
    Mockito.verify(mockExecutorServiceClient, Mockito.times(timesFailures + timesSuccess))
      .cancelFragments(Mockito.any(), Mockito.any());
  }

  /**
   * Check that a dead node doesn't not trigger a successful query notification if
   * node managing the last major fragments (see DX-10956)
   */

  @Test
  public void testNodeDead() {
    InOrder inOrder = Mockito.inOrder(completionListener);
    FragmentTracker fragmentTracker = new FragmentTracker(queryId, completionListener,
      () -> {}, null,
      new LocalExecutorSetService(DirectProvider.wrap(coordinator),
                                  DirectProvider.wrap(optionManager)), closeableSchedulerThreadPool);

    PlanFragmentFull fragment = new PlanFragmentFull(
      PlanFragmentMajor.newBuilder()
        .setHandle(FragmentHandle.newBuilder().setMajorFragmentId(0).setQueryId(queryId).build())
        .build(),
      PlanFragmentMinor.newBuilder()
        .setAssignment(selfEndpoint)
        .build());

    ExecutionPlan executionPlan = new ExecutionPlan(queryId, new Screen(OpProps.prototype(), null), 0, Collections
      .singletonList(fragment), null);
    observer.planCompleted(executionPlan);
    fragmentTracker.populate(executionPlan.getFragments(), new ResourceSchedulingDecisionInfo());

    // Notify node is dead
    fragmentTracker.handleFailedNodes(ImmutableSet.of(selfEndpoint));

    // Ideally, we should not even call succeeded...
    inOrder.verify(completionListener).failed(any(Exception.class));
    inOrder.verify(completionListener).succeeded();
  }

  // Verify that FragmentTracker can handle the case that NodeEndpoint has additional
  // info.
  @Test
  public void testNodeDeadWithVerboseEndpoint() {
    InOrder inOrder = Mockito.inOrder(completionListener);
    FragmentTracker fragmentTracker = new FragmentTracker(queryId, completionListener,
      () -> {}, null,
      new LocalExecutorSetService(DirectProvider.wrap(coordinator),
                                  DirectProvider.wrap(optionManager)), closeableSchedulerThreadPool);

    PlanFragmentFull fragment = new PlanFragmentFull(
      PlanFragmentMajor.newBuilder()
        .setHandle(FragmentHandle.newBuilder().setMajorFragmentId(0).setQueryId(queryId).build())
        .build(),
      PlanFragmentMinor.newBuilder()
        .setAssignment(selfEndpoint)
        .build());

    ExecutionPlan executionPlan = new ExecutionPlan(queryId, new Screen(OpProps.prototype(), null), 0, Collections
      .singletonList(fragment), null);
    observer.planCompleted(executionPlan);
    fragmentTracker.populate(executionPlan.getFragments(), new ResourceSchedulingDecisionInfo());

    // Notify node is dead, with verbose endpoint
    NodeEndpoint verboseEndpoint = NodeEndpoint.newBuilder()
      .mergeFrom(selfEndpoint)
      .setNodeTag("mytag")
      .setAvailableCores(10)
      .build();
    fragmentTracker.handleFailedNodes(ImmutableSet.of(verboseEndpoint));

    inOrder.verify(completionListener).failed(any(Exception.class));
    inOrder.verify(completionListener).succeeded();
  }

  @Mock
  Runnable queryCloser;

  @Test
  public void testEmptyFragmentList() {
    InOrder inOrder = Mockito.inOrder(completionListener, queryCloser);
    FragmentTracker fragmentTracker = new FragmentTracker(queryId, completionListener,
      queryCloser, null,
      new LocalExecutorSetService(DirectProvider.wrap(coordinator),
                                  DirectProvider.wrap(optionManager)), closeableSchedulerThreadPool);

    fragmentTracker.populate(Collections.emptyList(), new ResourceSchedulingDecisionInfo());

    inOrder.verify(completionListener).succeeded();
    inOrder.verify(queryCloser).run();
  }
}
