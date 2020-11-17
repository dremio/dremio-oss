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

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.maestro.planner.ExecutionPlanCreator;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.ExecutionPlanningResources;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.foreman.CompletionListener;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.ResourceSet;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class QueryTrackerImpl implements QueryTracker {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(QueryTrackerImpl.class);
  private static final ControlsInjector injector =
    ControlsInjectorFactory.getInjector(QueryTrackerImpl.class);

  @VisibleForTesting
  public static final String INJECTOR_EXECUTION_PLANNING_ERROR = "executionPlanningError";

  @VisibleForTesting
  public static final String INJECTOR_EXECUTION_PLANNING_PAUSE = "executionPlanningPause";

  @VisibleForTesting
  public static final String INJECTOR_NODE_COMPLETION_ERROR = "nodeCompletionError";

  @VisibleForTesting
  public static final String INJECTOR_STARTING_PAUSE = "startingPause";

  private final QueryId queryId;
  private final QueryContext context;
  private final PhysicalPlanReader reader;
  private final ResourceAllocator queryResourceManager;
  private final ExecutorSelectionService executorSelectionService;
  private final MaestroObserver observer;
  private final ExecutorServiceClientFactory executorServiceClientFactory;
  private final JobTelemetryClient jobTelemetryClient;

  private final FragmentTracker fragmentTracker;
  private volatile ProgressTracker progressTracker;
  private volatile PhysicalPlan physicalPlan;
  private volatile ExecutionPlan executionPlan;
  private volatile ResourceTracker resourceTracker;
  private volatile ExecutionPlanningResources executionPlanningResources;

  QueryTrackerImpl(
    QueryId queryId,
    QueryContext context,
    PhysicalPlan physicalPlan,
    PhysicalPlanReader reader,
    ResourceAllocator queryResourceManager,
    ExecutorSetService executorSetService,
    ExecutorSelectionService executorSelectionService,
    ExecutorServiceClientFactory executorServiceClientFactory,
    JobTelemetryClient jobTelemetryClient,
    MaestroObserver observer,
    CompletionListener listener,
    Runnable queryCloser) {

    this.queryId = queryId;
    this.context = context;
    this.physicalPlan = physicalPlan;
    this.reader = reader;
    this.queryResourceManager = queryResourceManager;
    this.executorSelectionService = executorSelectionService;
    this.executorServiceClientFactory = executorServiceClientFactory;
    this.jobTelemetryClient = jobTelemetryClient;
    this.observer = observer;

    this.fragmentTracker = new FragmentTracker(queryId, listener,
      queryCloser, executorServiceClientFactory, executorSetService);
  }

  @Override
  public void allocateResources() throws ExecutionSetupException, ResourceAllocationException  {
    resourceTracker = new ResourceTracker(physicalPlan, context, queryResourceManager, observer);
  }

  @Override
  public void planExecution() throws ExecutionSetupException {
    executionPlanningResources = ExecutionPlanCreator.getParallelizationInfo(context, observer,
      physicalPlan, executorSelectionService, resourceTracker.getResourceSchedulingDecisionInfo());

    injector.injectChecked(context.getExecutionControls(),
      INJECTOR_EXECUTION_PLANNING_ERROR, ExecutionSetupException.class);
    injector.injectPause(context.getExecutionControls(),
      INJECTOR_EXECUTION_PLANNING_PAUSE, logger);

    executionPlan = ExecutionPlanCreator.getExecutionPlan(context, reader, observer, physicalPlan,
      resourceTracker.getResources(),
      executionPlanningResources.getPlanningSet(), executorSelectionService,
      resourceTracker.getResourceSchedulingDecisionInfo(),
      executionPlanningResources.getGroupResourceInformation());
    observer.planCompleted(executionPlan);
    physicalPlan = null; // no longer needed
  }

  @Override
  public void startFragments() throws ExecutionSetupException {
    Preconditions.checkNotNull(executionPlan, "execution plan required");

    // Populate fragments before sending the query fragments.
    fragmentTracker.populate(executionPlan.getFragments(), resourceTracker.getResourceSchedulingDecisionInfo());

    AbstractMaestroObserver fragmentActivateObserver  = new AbstractMaestroObserver() {
      @Override
      public void activateFragmentFailed(Exception ex) {
        fragmentTracker.sendOrActivateFragmentsFailed(ex);
      }
    };

    injector.injectPause(context.getExecutionControls(),
      INJECTOR_STARTING_PAUSE, logger);
    try {
      FragmentStarter starter = new FragmentStarter(executorServiceClientFactory,
        resourceTracker.getResourceSchedulingDecisionInfo(),
        context.getExecutionControls());
      starter.start(executionPlan, MaestroObservers.of(observer, fragmentActivateObserver));
      executionPlan = null; // no longer needed

      progressTracker = new ProgressTracker(queryId, jobTelemetryClient, observer);
    } catch (Exception ex) {
      fragmentTracker.sendOrActivateFragmentsFailed(ex);
      throw ex;
    }
  }

  @Override
  public void nodeCompleted(NodeQueryCompletion completion) throws RpcException {
    injector.injectChecked(context.getExecutionControls(),
      INJECTOR_NODE_COMPLETION_ERROR, RpcException.class);

    fragmentTracker.nodeCompleted(completion);
  }

  @Override
  public void screenCompleted(NodeQueryScreenCompletion completion) {
    fragmentTracker.screenCompleted();
  }

  @Override
  public void nodeMarkFirstError(NodeQueryFirstError firstError) {
    fragmentTracker.nodeMarkFirstError(firstError);
  }

  @Override
  public void cancel() {
    fragmentTracker.cancelExecutingFragments();
  }

  @VisibleForTesting
  ResourceSet getResources() {
    return resourceTracker.getResources();
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(resourceTracker, fragmentTracker, progressTracker,
      executionPlanningResources);
  }

}
