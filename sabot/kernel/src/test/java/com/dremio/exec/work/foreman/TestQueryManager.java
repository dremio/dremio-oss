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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.config.Screen;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.observer.AttemptObservers;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos.FragmentHandle;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.store.sys.accel.AccelerationDetailsPopulator;
import com.dremio.exec.store.sys.accel.AccelerationManager;
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

  }

  /**
   * Check that a dead node doesn't not trigger a successful query notification if
   * node managing the last major fragments (see DX-10956)
   */
  @Test
  public void testNodeDead() {
    InOrder inOrder = Mockito.inOrder(completionListener);
    AttemptObservers observers = AttemptObservers.of(observer);
    QueryManager queryManager = new QueryManager(queryId, context, completionListener, new Pointer<QueryId>(), observers, true, catalog);

    final NodeEndpoint endpoint = NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();
    PlanFragment fragment = PlanFragment.newBuilder()
        .setAssignment(endpoint)
        .setHandle(FragmentHandle.newBuilder().setMajorFragmentId(0).setQueryId(queryId).build())
        .build();

    ExecutionPlan executionPlan = new ExecutionPlan(new Screen(null), 0, Arrays.asList(fragment));
    observers.planCompleted(executionPlan);

    // Notify node is dead
    queryManager.getNodeStatusListener().nodesUnregistered(ImmutableSet.of(endpoint));

    // Ideally, we should not even call succeeded...
    inOrder.verify(completionListener).failed(any(Exception.class));
    inOrder.verify(completionListener).succeeded();
  }
}
