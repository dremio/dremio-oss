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
package com.dremio.exec.work.foreman;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataStatsCollector;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMajor;
import com.dremio.exec.proto.CoordExecRPC.PlanFragmentMinor;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.options.OptionList;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.Pointer;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableSet;

import io.protostuff.ByteString;

/**
 * Test for {@code QueryManager}
 */
public class TestQueryManager extends DremioTest {

  private final QueryId queryId = QueryId
      .newBuilder()
      .setPart1(123L)
      .setPart2(456L)
      .build();

  @Mock
  private QueryContext context;

  @Mock
  private CompletionListener completionListener;

  @Mock
  private AttemptObserver observer;

  @Mock
  private Catalog catalog;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Boilerplate
    AccelerationManager accelerationManager = mock(AccelerationManager.class);
    AccelerationDetailsPopulator populator = mock(AccelerationDetailsPopulator.class);
    when(populator.computeAcceleration()).thenReturn(ByteString.EMPTY_BYTE_ARRAY);
    when(accelerationManager.newPopulator()).thenReturn(populator);
    when(context.getAccelerationManager()).thenReturn(accelerationManager);
    when(context.getQueryUserName()).thenReturn("myuser");
    when(context.getSession()).thenReturn(UserSession.Builder.newBuilder().build());
    when(context.getNonDefaultOptions()).thenReturn(new OptionList());
    when(catalog.getMetadataStatsCollector()).thenReturn(new MetadataStatsCollector());
  }

  /**
   * Check that a dead node doesn't not trigger a successful query notification if
   * node managing the last major fragments (see DX-10956)
   */
  @Test
  public void testNodeDead() {
    InOrder inOrder = Mockito.inOrder(completionListener);
    AttemptObservers observers = AttemptObservers.of(observer);
    QueryManager queryManager = new QueryManager(queryId, context, null, completionListener, new Pointer<>(), observers, true, true, catalog);

    final NodeEndpoint endpoint = NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();
    PlanFragmentFull fragment = new PlanFragmentFull(
      PlanFragmentMajor.newBuilder()
        .setHandle(FragmentHandle.newBuilder().setMajorFragmentId(0).setQueryId(queryId).build())
        .build(),
      PlanFragmentMinor.newBuilder()
        .setAssignment(endpoint)
        .build());

    ExecutionPlan executionPlan = new ExecutionPlan(new Screen(OpProps.prototype(), null), 0, Collections.singletonList(fragment), null);
    observers.planCompleted(executionPlan);

    // Notify node is dead
    queryManager.getNodeStatusListener().nodesUnregistered(ImmutableSet.of(endpoint));

    // Ideally, we should not even call succeeded...
    inOrder.verify(completionListener).failed(any(Exception.class));
    inOrder.verify(completionListener).succeeded();
  }

  @Test
  public void testResourceSchedulingInProfile() throws Exception {
    AttemptObservers observers = AttemptObservers.of(observer);

    final NodeEndpoint endpoint = NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();

    when(context.getCurrentEndpoint()).thenReturn(endpoint);

    QueryManager queryManager = new QueryManager(queryId, context, null, completionListener, new Pointer<>(), observers, true, true, catalog);

    ResourceSchedulingDecisionInfo result = new ResourceSchedulingDecisionInfo();
    result.setQueueId("abcd");
    result.setQueueName("queue.abcd");
    observers.resourcesScheduled(result);

    UserBitShared.QueryProfile queryProfile =
      queryManager.getQueryProfile("my description", UserBitShared.QueryResult.QueryState.RUNNING, null,
        "some reason");

    assertNotNull(queryProfile.getResourceSchedulingProfile());
    assertEquals("abcd", queryProfile.getResourceSchedulingProfile().getQueueId());
    assertEquals("queue.abcd", queryProfile.getResourceSchedulingProfile().getQueueName());
  }
}
