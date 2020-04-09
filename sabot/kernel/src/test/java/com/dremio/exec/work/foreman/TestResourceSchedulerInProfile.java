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
package com.dremio.exec.work.foreman;

import java.util.Collections;
import java.util.function.Consumer;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataStatsCollector;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.observer.AbstractAttemptObserver;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSchedulingResult;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.sabot.rpc.user.UserSession;
import com.dremio.test.DremioTest;
import com.google.common.util.concurrent.Futures;

public class TestResourceSchedulerInProfile extends DremioTest {
  @Mock
  private OptionManager optionManager;

  @Mock
  private QueryContext context;

  @Mock
  private AccelerationManager accelerationManager;

  @Mock
  private Catalog catalog;

  @Mock
  private MetadataStatsCollector metadataStatsCollector;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    // Boilerplate
    Mockito.when(context.getQueryUserName()).thenReturn("myuser");

    Mockito.when(context.getSession()).thenReturn(UserSession.Builder.newBuilder().build());
    Mockito.when(context.getConfig()).thenReturn(SabotConfig.create());
    Mockito.when(context.getWorkloadType()).thenReturn(UserBitShared.WorkloadType.JDBC);
    Mockito.when(context.getOptions()).thenReturn(optionManager);
    Mockito.when(context.getAccelerationManager()).thenReturn(accelerationManager);
    Mockito.when(context.getCatalog()).thenReturn(catalog);
    Mockito.when(accelerationManager.newPopulator()).thenReturn(null);
    Mockito.when(catalog.getMetadataStatsCollector()).thenReturn(metadataStatsCollector);
    Mockito.when(metadataStatsCollector.getPlanPhaseProfiles()).thenReturn(Collections.emptyList());
  }

  @Test
  public void testResourceSchedulingInProfile() throws Exception {
    UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
      .setPart1(10)
      .setPart2(20)
      .build();

    final NodeEndpoint endpoint = NodeEndpoint.newBuilder().setAddress("host1").setFabricPort(12345).build();

    Mockito.when(context.getCurrentEndpoint()).thenReturn(endpoint);

    ResourceAllocator ra = new ResourceAllocator() {
      @Override
      public ResourceSchedulingResult allocate(ResourceSchedulingContext queryContext,
        ResourceSchedulingProperties resourceSchedulingProperties,
        Consumer<ResourceSchedulingDecisionInfo> resourceDecisionConsumer) {

        final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo = new ResourceSchedulingDecisionInfo();
        resourceSchedulingDecisionInfo.setQueueId("abcd");
        resourceSchedulingDecisionInfo.setQueueName("queue.abcd");
        resourceDecisionConsumer.accept(resourceSchedulingDecisionInfo);

        return new ResourceSchedulingResult(resourceSchedulingDecisionInfo,
          Futures.immediateFuture(new ResourceSet.ResourceSetNoOp()));
      }

      @Override
      public void start() throws Exception {

      }

      @Override
      public void close() throws Exception {

      }
    };

    AttemptProfileTracker tracker = new AttemptProfileTracker(queryId,
      context, "test",
      () -> UserBitShared.QueryResult.QueryState.COMPLETED,
      AbstractAttemptObserver.NOOP, null);
    ResourceSchedulingDecisionInfo info =
      ra.allocate(null, null, x -> {}).getResourceSchedulingDecisionInfo();
    tracker.getObserver().resourcesScheduled(info);

    UserBitShared.QueryProfile queryProfile = tracker.getPlanningProfile();

    Assert.assertNotNull(queryProfile.getResourceSchedulingProfile());
    Assert.assertEquals("abcd", queryProfile.getResourceSchedulingProfile().getQueueId());
    Assert.assertEquals("queue.abcd", queryProfile.getResourceSchedulingProfile().getQueueName());
  }
}
