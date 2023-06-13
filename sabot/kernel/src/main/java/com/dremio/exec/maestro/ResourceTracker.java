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

import java.util.concurrent.ExecutionException;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.util.Utilities;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSchedulingDecisionInfo;
import com.dremio.resource.ResourceSchedulingObserver;
import com.dremio.resource.ResourceSchedulingProperties;
import com.dremio.resource.ResourceSchedulingResult;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.resource.exception.ResourceUnavailableException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

/**
 * Tracks cluster/queue resources held by the query during execution.
 */
public class ResourceTracker implements AutoCloseable {
  private static final org.slf4j.Logger logger =
    org.slf4j.LoggerFactory.getLogger(ResourceTracker.class);
  private static final ControlsInjector injector =
    ControlsInjectorFactory.getInjector(ResourceTracker.class);

  private final ResourceAllocator resourceAllocator;
  private final QueryContext context;
  private volatile ResourceSet resourceSet;
  private ResourceSchedulingDecisionInfo resourceSchedulingDecisionInfo;

  @VisibleForTesting
  public static final String INJECTOR_RESOURCE_ALLOCATE_ERROR = "resourceAllocateError";

  @VisibleForTesting
  public static final String INJECTOR_RESOURCE_ALLOCATE_UNAVAILABLE_ERROR = "resourceAllocateUnavailableError";

  @VisibleForTesting
  public static final String INJECTOR_RESOURCE_ALLOCATE_PAUSE = "resourceAllocatePause";

  @VisibleForTesting
  public static final String INJECTOR_ENGINE_START_PAUSE = "engine-start-pause";

  @VisibleForTesting
  public static final String INJECTOR_QUEUED_PAUSE = "queued-pause";

  ResourceTracker(QueryContext context, ResourceAllocator resourceAllocator) {
    this.resourceAllocator = resourceAllocator;
    this.resourceSet = null;
    this.context = context;
  }

  /**
   * Interrupts the allocation, esp if it is in any of the wait states.
   * <p>
   * Typically called when the cancel thread knows from the execution stage that we are somewhere in the
   * allocator.
   * </p>
   */
  void interruptAllocation() {
    resourceAllocator.cancel(context);
  }

  void allocate(
    PhysicalPlan physicalPlan,
    MaestroObserver observer) throws ExecutionSetupException, ResourceAllocationException {

    final double planCost = physicalPlan.getCost();
    ResourceSchedulingProperties resourceSchedulingProperties = new ResourceSchedulingProperties();
    resourceSchedulingProperties.setQueryCost(planCost);
    resourceSchedulingProperties.setRoutingQueue(context.getSession().getRoutingQueue());
    resourceSchedulingProperties.setRoutingTag(context.getSession().getRoutingTag());
    resourceSchedulingProperties.setQueryType(Utilities.getHumanReadableWorkloadType(context.getWorkloadType()));
    resourceSchedulingProperties.setRoutingEngine(context.getSession().getRoutingEngine());
    resourceSchedulingProperties.setQueryLabel(context.getSession().getQueryLabel());

    injector.injectChecked(context.getExecutionControls(), INJECTOR_RESOURCE_ALLOCATE_ERROR,
      IllegalStateException.class);
    injector.injectChecked(context.getExecutionControls(), INJECTOR_RESOURCE_ALLOCATE_UNAVAILABLE_ERROR,
      ResourceUnavailableException.class);
    injector.injectPause(context.getExecutionControls(), INJECTOR_RESOURCE_ALLOCATE_PAUSE, logger);

    ResourceSchedulingObserver resourceSchedulingObserver = new ResourceSchedulingObserver() {
      @Override
      public void beginEngineStart() {
        observer.beginState(AttemptObserver.toEvent(AttemptEvent.State.ENGINE_START));
        injector.injectPause(context.getExecutionControls(), INJECTOR_ENGINE_START_PAUSE, logger);
      }

      @Override
      public void beginQueueWait() {
        observer.beginState(AttemptObserver.toEvent(AttemptEvent.State.QUEUED));
        injector.injectPause(context.getExecutionControls(), INJECTOR_QUEUED_PAUSE, logger);
      }
    };
    long startTimeMs = System.currentTimeMillis();
    ResourceSchedulingResult resourceSchedulingResult = resourceAllocator.allocate(context, resourceSchedulingProperties,
      resourceSchedulingObserver,
      (x) -> {
      resourceSchedulingDecisionInfo = x;
      resourceSchedulingDecisionInfo.setResourceSchedulingProperties(resourceSchedulingProperties);
      resourceSchedulingDecisionInfo.setSchedulingStartTimeMs(startTimeMs);
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);
    });
    // should not put timeout, as we may be waiting for leases if query has to wait because queries concurrency limit
    try {
      resourceSet = resourceSchedulingResult.getResourceSetFuture().get();
      resourceSchedulingDecisionInfo.setSchedulingEndTimeMs(System.currentTimeMillis());
      observer.resourcesScheduled(resourceSchedulingDecisionInfo);

    } catch (ExecutionException|InterruptedException e) {
      // if the execution exception was caused by a ResourceAllocationException, throw the cause instead
      Throwables.propagateIfPossible(e.getCause(), ResourceAllocationException.class);
      // otherwise, wrap into an ExecutionSetupException
      throw new ExecutionSetupException("Unable to acquire slot for query.", e.getCause());
    }
  }

  ResourceSchedulingDecisionInfo getResourceSchedulingDecisionInfo() {
    return resourceSchedulingDecisionInfo;
  }

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
