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
import java.util.concurrent.ExecutionException;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.util.Utilities;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.foreman.ForemanSetupException;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSchedulingResult;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.exception.ResourceAllocationException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * Base class for Asynchronous queries.
 */
public abstract class AsyncCommand implements CommandRunner<Void> {

//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AsyncCommand.class);

  protected final QueryContext context;
  private final PhysicalPlanReader reader;
  private final CoordToExecTunnelCreator tunnelCreator;

  protected ResourceAllocator queryResourceManager;
  protected AttemptObserver observer;
  protected ResourceSet resourceSet;
  protected ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;

  private PlanningSet planningSet;
  private ExecutionPlan executionPlan;

  public AsyncCommand(QueryContext context, ResourceAllocator queryResourceManager, AttemptObserver observer,
      PhysicalPlanReader reader, CoordToExecTunnelCreator tunnelCreator) {
    this.context = context;
    this.queryResourceManager = queryResourceManager;
    this.observer = observer;
    this.reader = reader;
    this.tunnelCreator = tunnelCreator;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ASYNC_QUERY;
  }

  protected abstract PhysicalPlan getPhysicalPlan();

  public void allocateResources() throws Exception {
    planningSet = allocateResourcesBasedOnPlan(getPhysicalPlan());
  }

  public void planExecution() throws ExecutionSetupException {
    Preconditions.checkNotNull(planningSet, "planningSet required");

    executionPlan = ExecutionPlanCreator.getExecutionPlan(context, reader, observer, getPhysicalPlan(), resourceSet, planningSet);
    observer.planCompleted(executionPlan);
    planningSet = null; // no longer needed
  }

  public void startFragments() throws Exception {
    Preconditions.checkNotNull(executionPlan, "execution plan required");

    FragmentStarter starter = new FragmentStarter(tunnelCreator, resourceSchedulingDecisionInfo);
    starter.start(executionPlan, observer);
    executionPlan = null; // no longer needed
  }

  @Override
  public Void execute() {
    //TODO (DX-16022) refactor the code to no longer require this
    throw new IllegalStateException("Should never be called");
  }

  /**
   * To get resources needed based on Parallelization of the PhysicalPlan
   * @param plan
   * @return PlanningSet
   * @throws Exception
   */
  protected PlanningSet allocateResourcesBasedOnPlan(PhysicalPlan plan) throws Exception {
    final double planCost = plan.getCost();
    final Collection<CoordinationProtos.NodeEndpoint> activeEndpoints = context.getActiveEndpoints();
    final PlanningSet planningSet = ExecutionPlanCreator.getParallelizationInfo(context, observer, plan,
      activeEndpoints);
    // map from major fragment to map of endpoint to number of occurrences of that endpoint
    Map<Integer, Map<CoordinationProtos.NodeEndpoint, Integer>> endpoints =
      ResourceAllocationUtils.getEndPoints(planningSet);
    ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setResourceData(endpoints);
    resourceSchedulingProperties.setQueryCost(planCost);
    resourceSchedulingProperties.setQueryType(Utilities.getHumanReadableWorkloadType(context.getWorkloadType()));

    long startTimeMs = System.currentTimeMillis();
    ResourceSchedulingResult resourceSchedulingResult = queryResourceManager.allocate(context, resourceSchedulingProperties);
    resourceSchedulingDecisionInfo = resourceSchedulingResult.getResourceSchedulingDecisionInfo();
    resourceSchedulingDecisionInfo.setResourceSchedulingProperties(resourceSchedulingProperties);
    resourceSchedulingDecisionInfo.setSchedulingStartTimeMs(startTimeMs);
    observer.resourcesScheduled(resourceSchedulingDecisionInfo);
    // should not put timeout, as we may be waiting for leases if query has to wait because queries concurrency limit
    try {
      resourceSet = resourceSchedulingResult.getResourceSetFuture().get();
      resourceSchedulingDecisionInfo.setSchedulingEndTimeMs(System.currentTimeMillis());
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);
    } catch (ExecutionException e) {
      // if the execution exception was caused by a ResourceAllocationException, throw the cause instead
      Throwables.propagateIfPossible(e.getCause(), ResourceAllocationException.class);
      // otherwise, wrap into a ForemanSetupException
      throw new ForemanSetupException("Unable to acquire slot for query.", e.getCause());
    }

    return planningSet;
  }

  @VisibleForTesting
  ResourceSet getResources() {
    return resourceSet;
  }

  @Override
  public void close() throws Exception {
    if (resourceSet != null) {
      resourceSet.close();
    }
  }
}
