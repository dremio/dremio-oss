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
package com.dremio.resource.basic;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import javax.inject.Provider;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceAllocation;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Basic implementation of Resource Allocation APIs
 */
public class BasicResourceAllocator implements ResourceAllocator {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BasicResourceAllocator.class);

  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private ClusterCoordinator clusterCoordinator;
  private final ListeningExecutorService executorService = MoreExecutors.newDirectExecutorService();
  private final ConcurrentMap<UserBitShared.QueryId, DistributedSemaphore.DistributedLease> queryToLease = Maps
    .newConcurrentMap();
  private final ConcurrentMap<UserBitShared.QueryId, List<ResourceSet>> resourceAllocations = Maps
    .newConcurrentMap();

  public BasicResourceAllocator(final Provider<ClusterCoordinator> clusterCoordinatorProvider) {
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
  }

  @Override
  public ListenableFuture<ResourceSet> allocate(final ResourceSchedulingContext queryContext,
                                                final ResourceSchedulingProperties resourceSchedulingProperties) {

    ListenableFuture<ResourceSet> futureAllocation = executorService.submit(() -> {
      final QueueType queueType = getQueueNameFromSchedulingProperties(queryContext, resourceSchedulingProperties);
      final DistributedSemaphore.DistributedLease lease;
      // new query
      if (!queryToLease.containsKey(queryContext.getQueryId())) {
        lease = acquireQuerySemaphoreIfNecessary(queryContext, queueType);
        if (lease != null) {
          queryToLease.putIfAbsent(queryContext.getQueryId(), lease);
        }
        resourceAllocations.putIfAbsent(queryContext.getQueryId(), Lists.newArrayList());
      } else {
        lease = queryToLease.get(queryContext.getQueryId());
      }

      // update query limit based on the queueType
      final OptionManager options = queryContext.getOptions();
      final boolean memoryControlEnabled = options.getOption(BasicResourceConstants.ENABLE_QUEUE_MEMORY_LIMIT);
      // TODO REFLECTION_SMALL, REFLECTION_LARGE was not there before - was it a bug???
      final long memoryLimit = (queueType == QueueType.SMALL || queueType == QueueType.REFLECTION_SMALL) ?
        options.getOption(BasicResourceConstants.SMALL_QUEUE_MEMORY_LIMIT):
        options.getOption(BasicResourceConstants.LARGE_QUEUE_MEMORY_LIMIT);
      long queryMaxAllocation = queryContext.getQueryContextInfo().getQueryMaxAllocation();
      if (memoryControlEnabled && memoryLimit > 0) {
        queryMaxAllocation = Math.min(memoryLimit, queryMaxAllocation);
      }
      final UserBitShared.QueryId queryId = queryContext.getQueryId();
      final long queryMaxAllocationFinal = queryMaxAllocation;

      Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> resourcesPerNodePerMajor =
        resourceSchedulingProperties.getResourceData();

      final ResourceSet resourceSet = new BasicResourceSet(
        queryId,
        lease,
        resourcesPerNodePerMajor,
        queryMaxAllocationFinal);

      resourceAllocations.get(queryId).add(resourceSet);
      return resourceSet;
    });
    return futureAllocation;
  }

  protected QueueType getQueueNameFromSchedulingProperties(final ResourceSchedulingContext queryContext,
                                                        final ResourceSchedulingProperties resourceSchedulingProperties) {
    final Double cost = resourceSchedulingProperties.getQueryCost();

    Preconditions.checkNotNull(cost, "Queue Cost is not provided, Unable to determine " +
      "queue.");

    final long queueThreshold = queryContext.getOptions().getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE);
    final QueueType queueType;
    if (queryContext.getQueryContextInfo().getPriority().getWorkloadClass().equals(UserBitShared.WorkloadClass.BACKGROUND)) {
      queueType = (cost > queueThreshold) ? QueueType.REFLECTION_LARGE : QueueType.REFLECTION_SMALL;
    } else {
      queueType = (cost > queueThreshold) ? QueueType.LARGE : QueueType.SMALL;
    }
    return queueType;
  }

  @Override
  public void start() throws Exception {
    this.clusterCoordinator = clusterCoordinatorProvider.get();
  }

  @Override
  public void close() throws Exception {

  }

  private DistributedSemaphore.DistributedLease acquireQuerySemaphoreIfNecessary(final ResourceSchedulingContext queryContext,
                                                QueueType queueType) throws ResourceAllocationException {

    final OptionManager optionManager = queryContext.getOptions();

    boolean queuingEnabled = optionManager.getOption(BasicResourceConstants.ENABLE_QUEUE);
    boolean reflectionQueuingEnabled = optionManager.getOption(BasicResourceConstants.REFLECTION_ENABLE_QUEUE);

    if(!queuingEnabled){
      return null;
    }

    // switch back to regular queues if the reflection queuing is disabled
    QueueType adjustedQueueType = queueType;
    if (!reflectionQueuingEnabled) {
      if (queueType == QueueType.REFLECTION_LARGE) {
        adjustedQueueType = QueueType.LARGE;
      } else if (queueType == QueueType.REFLECTION_SMALL){
        adjustedQueueType = QueueType.SMALL;
      }
    }

    long queueTimeout = optionManager.getOption(BasicResourceConstants.QUEUE_TIMEOUT);
    final String queueName;

    DistributedSemaphore.DistributedLease lease;
    try {
      @SuppressWarnings("resource")
      final DistributedSemaphore distributedSemaphore;

      // get the appropriate semaphore
      switch (adjustedQueueType) {
        case LARGE:
          final int largeQueue = (int) optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.large", largeQueue);
          queueName = "large";
          break;
        case SMALL:
          final int smallQueue = (int) optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.small", smallQueue);
          queueName = "small";
          break;
        case REFLECTION_LARGE:
          final int reflectionLargeQueue = (int) optionManager.getOption(BasicResourceConstants.REFLECTION_LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("reflection.query.large", reflectionLargeQueue);
          queueName = "reflection_large";
          queueTimeout = optionManager.getOption(BasicResourceConstants.REFLECTION_QUEUE_TIMEOUT);
          break;
        case REFLECTION_SMALL:
          final int reflectionSmallQueue = (int) optionManager.getOption(BasicResourceConstants.REFLECTION_SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("reflection.query.small", reflectionSmallQueue);
          queueName = "reflection_small";
          queueTimeout = optionManager.getOption(BasicResourceConstants.REFLECTION_QUEUE_TIMEOUT);
          break;
        default:
          throw new ResourceAllocationException("Unsupported Queue type: " + adjustedQueueType);
      }
      lease = distributedSemaphore.acquire(queueTimeout, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      Throwables.propagateIfPossible(e);
      throw new ResourceAllocationException("Unable to acquire slot for query.", e);
    }

    if (lease == null) {
      throw UserException
        .resourceError()
        .message(
          "Unable to acquire queue resources for query within timeout.  Timeout for %s queue was set at %d seconds.",
          queueName, queueTimeout / 1000)
        .build(logger);
    }
    return lease;
  }

  /**
   * Need for testing purposes
   * @param endpoint
   * @param memoryLimit
   * @param majorFragment
   * @return
   */
  @VisibleForTesting
  public ResourceAllocation createAllocation(CoordinationProtos.NodeEndpoint endpoint, long memoryLimit, int
    majorFragment) {
    return new BasicResourceAllocation(endpoint, memoryLimit, majorFragment);
  }

  private class BasicResourceSet implements ResourceSet {

    private final UserBitShared.QueryId queryId;
    private final List<ResourceAllocation> resourceContainers = Lists.newArrayList();
    private volatile DistributedSemaphore.DistributedLease lease; // used to limit the number of concurrent queries
    private final long memoryLimit;

    BasicResourceSet(UserBitShared.QueryId queryId,
                     DistributedSemaphore.DistributedLease lease,
                     Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> resourcesPerNodePerMajor,
                     long memoryLimit) {
      this.queryId = queryId;
      this.lease = lease;
      this.memoryLimit = memoryLimit;
      for (Map.Entry<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> majorFragmentEntry : resourcesPerNodePerMajor.entrySet()) {
        for (Map.Entry<CoordinationProtos.NodeEndpoint, Integer> nodeEndpointEntry : majorFragmentEntry.getValue().entrySet()) {
          resourceContainers.add(
            new BasicResourceAllocation(nodeEndpointEntry.getKey(),
              memoryLimit,
              majorFragmentEntry.getKey()));
        }
      }
    }

    @Override
    public List<ResourceAllocation> getResourceAllocations() {
      synchronized (this) {
        return resourceContainers;
      }
    }

    @Override
    public void reassignMajorFragments(Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> majorToEndpoinsMap) {
      final List<ResourceAllocation> resourceContainers = Lists.newArrayList();
      for (Map.Entry<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> entry: majorToEndpoinsMap.entrySet()) {
        for (Map.Entry<CoordinationProtos.NodeEndpoint, Integer> nodeEndpointEntry : entry.getValue().entrySet()) {
          resourceContainers.add(
            new BasicResourceAllocation(nodeEndpointEntry.getKey(),
              memoryLimit,
              entry.getKey()));
        }
      }
      synchronized (this) {
        this.resourceContainers.clear();
        this.resourceContainers.addAll(resourceContainers);
      }
    }

    @Override
    public void close() throws IOException {
      // process data if needed and
      // remove itself from map
      queryToLease.remove(queryId);
      // close all resources for this query
      List<ResourceSet> allocations = resourceAllocations.get(queryId);
      for (ResourceSet allocationSet : allocations) {
        // close allocation by allocation - otherwise can go into infinite loop
        for (ResourceAllocation allocation : allocationSet.getResourceAllocations()) {
          allocation.close();
        }
      }
      for (ResourceAllocation allocation : resourceContainers) {
        allocation.close();
      }
      resourceAllocations.remove(queryId);
      // release semaphore
      releaseLease();
    }

    private void releaseLease() {
      while (lease != null) {
        try {
          lease.close();
          lease = null;
        } catch (final InterruptedException e) {
          // if we end up here, the while loop will try again
        } catch (final Exception e) {
          logger.warn("Failure while releasing lease.", e);
          break;
        }
      }
    }

  }

  private static class BasicResourceAllocation implements ResourceAllocation {

    private final CoordinationProtos.NodeEndpoint endpoint;
    private final long memoryLimit;
    private int majorFragment;

    BasicResourceAllocation(CoordinationProtos.NodeEndpoint endpoint, long memoryLimit, int majorFragment) {
      this.endpoint = endpoint;
      this.memoryLimit = memoryLimit;
      this.majorFragment = majorFragment;
    }

    @Override
    public CoordinationProtos.NodeEndpoint getEndPoint() {
      return endpoint;
    }

    @Override
    public int getMajorFragment() {
      return majorFragment;
    }

    @Override
    public long getMemory() {
      return memoryLimit;
    }

    @Override
    public long getId() {
      return 0;
    }

    @Override
    public void assignMajorFragment(int majorFragmentId) {
      this.majorFragment = majorFragmentId;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public String toString() {
      StringBuilder strB = new StringBuilder();
      strB.append("MajorFragmentId: ");
      strB.append(majorFragment);
      strB.append(", Memory: ");
      strB.append(memoryLimit);
      strB.append("\nEndpoint: ");
      strB.append(endpoint);
      strB.append("\n");
      return strB.toString();
    }
  }
}
