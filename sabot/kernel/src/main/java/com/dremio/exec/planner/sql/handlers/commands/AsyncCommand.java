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
package com.dremio.exec.planner.sql.handlers.commands;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.basic.BasicResourceConstants;
import com.dremio.resource.basic.QueueType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Base class for Asynchronous queries.
 */
public abstract class AsyncCommand<T> implements CommandRunner<T> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncCommand.class);

  protected final QueryContext context;

  private QueueType queueType;
  protected ResourceAllocator queryResourceManager;
  protected AttemptObserver observer;
  protected ResourceSet resourceSet;

  public AsyncCommand(QueryContext context, ResourceAllocator queryResourceManager, AttemptObserver observer) {
    this.context = context;
    this.queryResourceManager = queryResourceManager;
    this.observer = observer;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ASYNC_QUERY;
  }

  protected void setQueueTypeFromPlan(PhysicalPlan plan) {
    final long queueThreshold = context.getOptions().getOption(BasicResourceConstants.QUEUE_THRESHOLD_SIZE);
    if (context.getQueryContextInfo().getPriority().getWorkloadClass().equals(WorkloadClass.BACKGROUND)) {
      setQueueType((plan.getCost() > queueThreshold) ? QueueType.REFLECTION_LARGE : QueueType.REFLECTION_SMALL);
    } else {
      setQueueType((plan.getCost() > queueThreshold) ? QueueType.LARGE : QueueType.SMALL);
    }
  }

  private void setQueueType(QueueType queueType) {
    this.queueType = queueType;
  }

  public QueueType getQueueType() {
    return queueType;
  }

  /**
   * To get resources needed based on Parallelization of the PhysicalPlan
   * @param plan
   * @return PlanningSet
   * @throws Exception
   */
  protected PlanningSet allocateResourcesBasedOnPlan(PhysicalPlan plan) throws Exception {
    final double planCost = plan.getCost();
    setQueueTypeFromPlan(plan);
    final Collection<CoordinationProtos.NodeEndpoint> activeEndpoints = context.getActiveEndpoints();
    final PlanningSet planningSet = ExecutionPlanCreator.getParallelizationInfo(context, observer, plan,
      activeEndpoints);
    // map from major fragment to map of endpoint to number of occurrences of that endpoint
    Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> endpoints =
      ResourceAllocationUtils.getEndPoints(planningSet);
    ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setResourceData(endpoints);
    resourceSchedulingProperties.setQueryCost(Double.valueOf(planCost));
    // TODO set client type, workload type???

    ListenableFuture<ResourceSet> resourcesFuture = queryResourceManager.allocate(context, resourceSchedulingProperties);
    resourceSet = resourcesFuture.get(15, TimeUnit.SECONDS);
    return planningSet;
  }

  @VisibleForTesting
  ResourceSet getResources() {
    return resourceSet;
  }

  @Override
  public void close() throws Exception {
    resourceSet.close();
  }
}
