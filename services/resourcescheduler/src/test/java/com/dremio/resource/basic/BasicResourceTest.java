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
package com.dremio.resource.basic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.curator.CuratorConnectionLossException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSchedulingResult;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * To test Basic Resource Allocations
 */
public class BasicResourceTest {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicResourceTest.class);

  @Rule
  public Timeout globalTimeout = new Timeout(30, TimeUnit.SECONDS);

  @Test
  public void basicResourceSchedulingPropertiesTest() throws Exception {
    ResourceSchedulingProperties basicProperties = new ResourceSchedulingProperties();

    basicProperties.setClientType("jdbc");
    basicProperties.setQueryCost(30000.0D);
    basicProperties.setQueueName("abc");
    basicProperties.setUser("bcd");
    basicProperties.setTag("tagvalue1");

    assertEquals(30000.0D,
      basicProperties.getQueryCost(),10E-6);

    assertEquals("jdbc", basicProperties.getClientType());
    assertEquals("abc", basicProperties.getQueueName());
    assertEquals("bcd", basicProperties.getUser());
    assertEquals("tagvalue1", basicProperties.getTag());

  }

  @Test
  public void testQueueingAllocations() throws Exception {
    final CoordinationProtos.NodeEndpoint nodeEndpoint = CoordinationProtos.NodeEndpoint.newBuilder()
      .setAddress("host1")
      .setFabricPort(1234)
      .setUserPort(2345)
      .setAvailableCores(3)
      .setMaxDirectMemory(8 * 1024)
      .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.EXECUTOR)))
      .build();

    final OptionManager optionManager = mock(OptionManager.class);

    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_ENABLE_QUEUE)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE_MEMORY_LIMIT)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_MEMORY_LIMIT)).thenReturn(4096L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_MEMORY_LIMIT)).thenReturn(Long.MAX_VALUE);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE)).thenReturn(30000000L);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_TIMEOUT)).thenReturn(1000L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_LARGE_QUEUE_SIZE)).thenReturn(1L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_SIZE)).thenReturn(10L);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_SIZE)).thenReturn(2L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_SMALL_QUEUE_SIZE)).thenReturn(10L);

    final ClusterCoordinator clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();

    final UserBitShared.QueryId queryId = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());
    final UserBitShared.QueryId queryId1 = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());
    final UserBitShared.QueryId queryId2 = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());

    final ResourceSchedulingContext resourceSchedulingContext = createQueryContext(queryId, optionManager,
      nodeEndpoint);

    final ResourceSchedulingContext resourceSchedulingContext1 = createQueryContext(queryId1, optionManager,
      nodeEndpoint);

    final ResourceSchedulingContext resourceSchedulingContext2 = createQueryContext(queryId2, optionManager,
      nodeEndpoint);

    final ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setQueryCost(112100D);

    final BasicResourceAllocator resourceAllocator = new BasicResourceAllocator(DirectProvider.wrap
      (clusterCoordinator));
    resourceAllocator.start();


    final ResourceSet resourceSet =
      resourceAllocator.allocate(resourceSchedulingContext, resourceSchedulingProperties).getResourceSetFuture().get();

    final ResourceSet resourceSet1 =
      resourceAllocator.allocate(resourceSchedulingContext1, resourceSchedulingProperties).getResourceSetFuture().get();

    try {
        resourceAllocator.allocate(resourceSchedulingContext2, resourceSchedulingProperties).getResourceSetFuture().get();
      fail("Should not be able to scheudle 3rd job with limit 2");
    } catch(ExecutionException e) {
      assertTrue(e.getCause() instanceof ResourceAllocationException);
      assertTrue(e.getMessage().contains("Workload Manager"));
    }

    assertEquals(4096, resourceSet.getPerNodeQueryMemoryLimit());
    resourceSet.close();

    final ResourceSet resourceSet2 =
      resourceAllocator.allocate(resourceSchedulingContext2, resourceSchedulingProperties).getResourceSetFuture().get();

    assertEquals(4096, resourceSet1.getPerNodeQueryMemoryLimit());
    resourceSet1.close();

    assertEquals(4096, resourceSet2.getPerNodeQueryMemoryLimit());
    resourceSet2.close();
  }

  @Test
  public void testQueueingSemaphoreException() throws Exception {
    final CoordinationProtos.NodeEndpoint nodeEndpoint = CoordinationProtos.NodeEndpoint.newBuilder()
      .setAddress("host1")
      .setFabricPort(1234)
      .setUserPort(2345)
      .setAvailableCores(3)
      .setMaxDirectMemory(8 * 1024)
      .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.EXECUTOR)))
      .build();

    final OptionManager optionManager = mock(OptionManager.class);

    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_ENABLE_QUEUE)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE_MEMORY_LIMIT)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_MEMORY_LIMIT)).thenReturn(4096L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_MEMORY_LIMIT)).thenReturn(Long.MAX_VALUE);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE)).thenReturn(30000000L);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_TIMEOUT)).thenReturn(1000L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_LARGE_QUEUE_SIZE)).thenReturn(1L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_SIZE)).thenReturn(10L);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_SIZE)).thenReturn(2L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_SMALL_QUEUE_SIZE)).thenReturn(10L);


    final UserBitShared.QueryId queryId2 = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());

    final ResourceSchedulingContext resourceSchedulingContext2 = createQueryContext(queryId2, optionManager,
      nodeEndpoint);

    final ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setQueryCost(112100D);

    DistributedSemaphore mockLease = mock(DistributedSemaphore.class);
    when(mockLease.acquire(1000L, TimeUnit.MILLISECONDS)).thenThrow(new CuratorConnectionLossException());

    ClusterCoordinator mockClusterCoordinator = mock(ClusterCoordinator.class);
    when(mockClusterCoordinator.getSemaphore("query.small", 2)).thenReturn(mockLease);

    final BasicResourceAllocator resourceAllocator =
      new BasicResourceAllocator(DirectProvider.wrap(mockClusterCoordinator));
    resourceAllocator.start();

    try {
      resourceAllocator.allocate(resourceSchedulingContext2, resourceSchedulingProperties).getResourceSetFuture().get();
      fail("Should not be able to schedule");
    } catch(ExecutionException e) {
      assertTrue(e.getCause() instanceof ResourceAllocationException);
      assertTrue(e.getMessage().contains("Workload Manager"));
    }
    resourceAllocator.close();
  }


  @Test
  public void testQueueingDisabledAllocations() throws Exception {

    final CoordinationProtos.NodeEndpoint nodeEndpoint1 = CoordinationProtos.NodeEndpoint.newBuilder()
      .setAddress("host1")
      .setFabricPort(1234)
      .setUserPort(2345)
      .setAvailableCores(3)
      .setMaxDirectMemory(8 * 1024)
      .setRoles(ClusterCoordinator.Role.toEndpointRoles(Sets.newHashSet(ClusterCoordinator.Role.EXECUTOR)))
      .build();

    final OptionManager optionManager = mock(OptionManager.class);

    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE)).thenReturn(false);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_ENABLE_QUEUE)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE_MEMORY_LIMIT)).thenReturn(true);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_MEMORY_LIMIT)).thenReturn(4096L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_MEMORY_LIMIT)).thenReturn(Long.MAX_VALUE);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE)).thenReturn(30000000L);
    when(optionManager.getOption(BasicResourceConstants.QUEUE_TIMEOUT)).thenReturn(60 * 1000 * 5L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_LARGE_QUEUE_SIZE)).thenReturn(1L);
    when(optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_SIZE)).thenReturn(10L);
    when(optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_SIZE)).thenReturn(100L);
    when(optionManager.getOption(BasicResourceConstants.REFLECTION_SMALL_QUEUE_SIZE)).thenReturn(10L);

    final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);

    final UserBitShared.QueryId queryId = ExternalIdHelper.toQueryId(ExternalIdHelper.generateExternalId());

    final ResourceSchedulingContext resourceSchedulingContext = createQueryContext(queryId, optionManager,
      nodeEndpoint1);

    final BasicResourceAllocator resourceAllocator = new BasicResourceAllocator(DirectProvider.wrap
      (clusterCoordinator));
    resourceAllocator.start();

    final ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setQueryCost(112100D);

    final ResourceSchedulingResult resourceSchedulingResult = resourceAllocator.allocate(resourceSchedulingContext,
      resourceSchedulingProperties);

    final ResourceSet resourceSet = resourceSchedulingResult.getResourceSetFuture().get();
    assertEquals(4096, resourceSet.getPerNodeQueryMemoryLimit());

    assertNull(resourceSchedulingResult.getResourceSchedulingDecisionInfo().getRuleContent());
    assertNull(resourceSchedulingResult.getResourceSchedulingDecisionInfo().getRuleId());
    assertNull(resourceSchedulingResult.getResourceSchedulingDecisionInfo().getRuleName());
    assertNotNull(resourceSchedulingResult.getResourceSchedulingDecisionInfo().getQueueName());
    assertNotNull(resourceSchedulingResult.getResourceSchedulingDecisionInfo().getWorkloadClass());

    assertEquals(UserBitShared.WorkloadClass.GENERAL,
      resourceSchedulingResult.getResourceSchedulingDecisionInfo().getWorkloadClass());
    assertEquals("SMALL", resourceSchedulingResult.getResourceSchedulingDecisionInfo().getQueueName());
    assertEquals("SMALL", resourceSchedulingResult.getResourceSchedulingDecisionInfo().getQueueId());
  }

  private ResourceSchedulingContext createQueryContext(final UserBitShared.QueryId queryId,
                                                       final OptionManager optionManager,
                                                       final CoordinationProtos.NodeEndpoint nodeEndpoint) {
    return new ResourceSchedulingContext() {

      @Override
      public CoordExecRPC.QueryContextInformation getQueryContextInfo() {
        return CoordExecRPC.QueryContextInformation.newBuilder()
          .setQueryMaxAllocation(Long.MAX_VALUE)
          .setPriority
            (
            CoordExecRPC.FragmentPriority.newBuilder().setWorkloadClass(UserBitShared.WorkloadClass.GENERAL).build()
            )
          .build();
      }

      @Override
      public UserBitShared.QueryId getQueryId() {
        return queryId;
      }

      @Override
      public String getQueryUserName() {
        return "foo";
      }

      @Override
      public CoordinationProtos.NodeEndpoint getCurrentEndpoint() {
        return nodeEndpoint;
      }

      @Override
      public Collection<CoordinationProtos.NodeEndpoint> getActiveEndpoints() {
        return ImmutableList.of(nodeEndpoint);
      }

      @Override
      public OptionManager getOptions() {
        return optionManager;
      }
    };
  }
}
