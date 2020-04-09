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

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.basic.BasicResourceAllocator;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.service.coordinator.ClusterCoordinator;

/**
 * Testing DX-16164 where we weren't setting queue name until after we had finished queueing
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
@RunWith(MockitoJUnitRunner.class)
public class TestQueuePosition {

  @Mock
  PhysicalPlan plan;
  @Mock
  QueryContext context;
  @Mock
  ClusterCoordinator clusterCoordinator;
  @Mock
  OptionManager optionsManager;
  @Mock
  MaestroObserver observer;

  @Mock
  UserSession session;

  @Test
  public void testQueue() throws Exception {
    final AtomicInteger count = new AtomicInteger();
    final ExecutionControls controls = new ExecutionControls(optionsManager,
      CoordinationProtos.NodeEndpoint.newBuilder()
        .setAddress("localhost")
        .setFabricPort(10000)
        .build());

    Mockito.when(context.getWorkloadType()).thenReturn(UserBitShared.WorkloadType.JDBC);
    Mockito.when(context.getOptions()).thenReturn(optionsManager);
    Mockito.when(context.getSession()).thenReturn(session);
    Mockito.when(context.getQueryContextInfo()).thenReturn(CoordExecRPC.QueryContextInformation.getDefaultInstance());
    Mockito.when(context.getExecutionControls()).thenReturn(controls);
    Mockito.doAnswer(invocation -> {
      int i = count.getAndIncrement();
      Object[] args = invocation.getArguments();
      check(i, (ResourceSchedulingDecisionInfo) args[0]);
      return null;
    }).when(observer).resourcesScheduled(Mockito.any());
    BasicResourceAllocator ra = new BasicResourceAllocator(() -> clusterCoordinator);
    ResourceTracker resourceTracker = new ResourceTracker(plan, context, ra, observer);
  }

  /**
   * We expect 2 calls.
   * 1. queue has been allocated but we have not been added to the queue
   * 3. queueing is finished and we can go to execution
   */
  private void check(int i, ResourceSchedulingDecisionInfo info) {
    switch (i) {
      case 0:
        Assert.assertEquals("SMALL", info.getQueueId());
        Assert.assertEquals("SMALL", info.getQueueName());
        Assert.assertTrue(0 < info.getSchedulingStartTimeMs());
        Assert.assertEquals(0, info.getSchedulingEndTimeMs());
        Assert.assertNotNull(info.getResourceSchedulingProperties());
        break;
      case 1:
        Assert.assertEquals("SMALL", info.getQueueId());
        Assert.assertEquals("SMALL", info.getQueueName());
        Assert.assertTrue(0 < info.getSchedulingStartTimeMs());
        Assert.assertTrue(0 < info.getSchedulingEndTimeMs());
        Assert.assertNotNull(info.getResourceSchedulingProperties());
        break;
      default:
        throw new UnsupportedOperationException("Did not expect more than 3 calls!");
    }
  }
}
