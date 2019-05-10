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
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.options.OptionManager;
import com.dremio.resource.ResourceAllocation;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSchedulingResult;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.dremio.service.Pointer;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.DistributedSemaphore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
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

  public BasicResourceAllocator(final Provider<ClusterCoordinator> clusterCoordinatorProvider) {
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
  }

  @Override
  public ResourceSchedulingResult allocate(final ResourceSchedulingContext queryContext,
                                                final ResourceSchedulingProperties resourceSchedulingProperties) {

    final ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo = new ResourceSchedulingDecisionInfo();
    final QueueType queueType = getQueueNameFromSchedulingProperties(queryContext, resourceSchedulingProperties);
    resourceSchedulingDecisionInfo.setQueueName(queueType.name());
    resourceSchedulingDecisionInfo.setQueueId(queueType.name());
    resourceSchedulingDecisionInfo.setWorkloadClass(queryContext.getQueryContextInfo().getPriority().getWorkloadClass());

    final Pointer<DistributedSemaphore.DistributedLease> lease = new Pointer();
    ListenableFuture<ResourceSet> futureAllocation = executorService.submit(() -> {
      lease.value = acquireQuerySemaphoreIfNecessary(queryContext, queueType);

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
        lease.value,
        resourcesPerNodePerMajor,
        queryMaxAllocationFinal,
        queueType.name());

      return resourceSet;
    });
    Futures.addCallback(futureAllocation, new FutureCallback<ResourceSet>() {
      @Override
      public void onSuccess(@Nullable ResourceSet resourceSet) {
        // don't need to do anything additional
      }

      @Override
      public void onFailure(Throwable throwable) {
        // need to close lease
        releaseLease(lease.value);
      }
    }, executorService);

    final ResourceSchedulingResult resourceSchedulingResult = new ResourceSchedulingResult(
      resourceSchedulingDecisionInfo,
      futureAllocation
    );
    return resourceSchedulingResult;
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
    String queueName = null;
    int maxRunningConcurrency = 0;

    DistributedSemaphore.DistributedLease lease;
    try {
      @SuppressWarnings("resource")
      final DistributedSemaphore distributedSemaphore;

      // get the appropriate semaphore
      switch (adjustedQueueType) {
        case LARGE:
          maxRunningConcurrency = (int) optionManager.getOption(BasicResourceConstants.LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.large", maxRunningConcurrency);
          queueName = "large";
          break;
        case SMALL:
          maxRunningConcurrency = (int) optionManager.getOption(BasicResourceConstants.SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("query.small", maxRunningConcurrency);
          queueName = "small";
          break;
        case REFLECTION_LARGE:
          maxRunningConcurrency = (int) optionManager.getOption(BasicResourceConstants.REFLECTION_LARGE_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("reflection.query.large", maxRunningConcurrency);
          queueName = "reflection_large";
          queueTimeout = optionManager.getOption(BasicResourceConstants.REFLECTION_QUEUE_TIMEOUT);
          break;
        case REFLECTION_SMALL:
          maxRunningConcurrency = (int) optionManager.getOption(BasicResourceConstants.REFLECTION_SMALL_QUEUE_SIZE);
          distributedSemaphore = clusterCoordinator.getSemaphore("reflection.query.small", maxRunningConcurrency);
          queueName = "reflection_small";
          queueTimeout = optionManager.getOption(BasicResourceConstants.REFLECTION_QUEUE_TIMEOUT);
          break;
        default:
          throw new ResourceAllocationException("Unsupported Queue type: " + adjustedQueueType);
      }
      lease = distributedSemaphore.acquire(queueTimeout, TimeUnit.MILLISECONDS);
    } catch (final Exception e) {
      final String message = String.format(
          "Query cancelled by Workload Manager. Cannot enqueue as the '%s' queue is full. Please try again later.",
          queueName);
      logger.trace(message, e);
      throw new ResourceUnavailableException(message);
    }

    if (lease == null) {
      final String message = String.format(
          "Query cancelled by Workload Manager. Query enqueued time of %.2f seconds exceeded for '%s' queue.",
          queueTimeout / 1000.0, queueName);
      logger.trace(message);
      throw new ResourceUnavailableException(message);
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
    private final String queueName;

    BasicResourceSet(UserBitShared.QueryId queryId,
                     DistributedSemaphore.DistributedLease lease,
                     Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> resourcesPerNodePerMajor,
                     long memoryLimit,
                     String queueName) {
      this.queryId = queryId;
      this.lease = lease;
      this.memoryLimit = memoryLimit;
      this.queueName = queueName;
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
      try {
        AutoCloseables.close(resourceContainers);
      } catch (IOException | RuntimeException e) {
        throw e;
      } catch (Exception e) {
        // should never happen because resourceContainers only throw IOException or unchecked exceptions, but in case...
        throw new RuntimeException(e);
      } finally {
        releaseLease(lease);
      }
    }
  }

  private static void releaseLease(DistributedSemaphore.DistributedLease lease) {
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
