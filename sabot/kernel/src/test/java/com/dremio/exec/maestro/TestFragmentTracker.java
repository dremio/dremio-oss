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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.common.config.SabotConfig;
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
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.dremio.service.coordinator.LocalExecutorSetService;
import com.dremio.service.coordinator.NodeStatusListener;
import com.dremio.service.coordinator.RegistrationHandle;
import com.dremio.service.coordinator.ServiceSet;
import com.google.common.collect.ImmutableSet;

public class TestFragmentTracker {
  private final QueryId queryId = QueryId
    .newBuilder()
    .setPart1(123L)
    .setPart2(456L)
    .build();

  private final NodeEndpoint selfEndpoint =
    NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();

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

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Boilerplate
    when(context.getQueryUserName()).thenReturn("myuser");
    when(context.getSession()).thenReturn(UserSession.Builder.newBuilder().build());
    when(context.getNonDefaultOptions()).thenReturn(new OptionList());
    when(context.getConfig()).thenReturn(SabotConfig.create());
    when(catalog.getMetadataStatsCollector()).thenReturn(new MetadataStatsCollector());

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

  /**
   * Check that a dead node doesn't not trigger a successful query notification if
   * node managing the last major fragments (see DX-10956)
   */

  @Test
  public void testNodeDead() {
    InOrder inOrder = Mockito.inOrder(completionListener);
    FragmentTracker fragmentTracker = new FragmentTracker(queryId, completionListener,
      () -> {}, null,
      new LocalExecutorSetService(DirectProvider.wrap(coordinator)));

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
      new LocalExecutorSetService(DirectProvider.wrap(coordinator)));

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
      new LocalExecutorSetService(DirectProvider.wrap(coordinator)));

    fragmentTracker.populate(Collections.emptyList(), new ResourceSchedulingDecisionInfo());

    inOrder.verify(completionListener).succeeded();
    inOrder.verify(queryCloser).run();
  }
}
