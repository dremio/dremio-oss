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

import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ExtendedLatch;
import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryCompletion;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryFirstError;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryScreenCompletion;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.rpc.RpcException;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.work.foreman.CompletionListener;
import com.dremio.resource.ResourceAllocator;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.sabot.rpc.ExecToCoordStatusHandler;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.execselector.ExecutorSelectionService;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.telemetry.api.metrics.Metrics;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Default implementation of MaestroService.
 */
public class MaestroServiceImpl implements MaestroService {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(MaestroServiceImpl.class);
  private static final ControlsInjector injector =
    ControlsInjectorFactory.getInjector(MaestroServiceImpl.class);

  @VisibleForTesting
  public static final String INJECTOR_EXECUTE_QUERY_BEGIN_ERROR = "executeQueryBeginError";

  @VisibleForTesting
  public static final String INJECTOR_EXECUTE_QUERY_END_ERROR = "executeQueryEndError";

  @VisibleForTesting
  public static final String INJECTOR_COMMAND_POOL_SUBMIT_ERROR  = "commandPoolSubmitError";

  private final Provider<ExecutorSetService> executorSetService;
  private final Provider<FabricService> fabric;
  private final Provider<SabotContext> sabotContext;
  private final Provider<CommandPool> commandPool;
  private final Provider<ResourceAllocator> resourceAllocator;
  private final Provider<ExecutorSelectionService> executorSelectionService;
  private final Provider<JobTelemetryClient> jobTelemetryClient;
  // single map of currently running queries
  private final ConcurrentMap<QueryId, QueryTracker> activeQueryMap = Maps.newConcurrentMap();

  private PhysicalPlanReader reader;
  private ExecToCoordStatusHandler execToCoordStatusHandlerImpl;
  private final Provider<ExecutorServiceClientFactory> executorServiceClientFactory;
  private ExtendedLatch exitLatch = null; // This is used to wait to exit when things are still running

  public MaestroServiceImpl(
    final Provider<ExecutorSetService> executorSetService,
    final Provider<FabricService> fabric,
    final Provider<SabotContext> sabotContext,
    final Provider<ResourceAllocator> resourceAllocator,
    final Provider<CommandPool> commandPool,
    final Provider<ExecutorSelectionService> executorSelectionService,
    final Provider<ExecutorServiceClientFactory> executorServiceClientFactory,
    final Provider<JobTelemetryClient> jobTelemetryClient) {

    this.executorSetService = executorSetService;
    this.fabric = fabric;
    this.sabotContext = sabotContext;
    this.commandPool = commandPool;
    this.executorSelectionService = executorSelectionService;
    this.resourceAllocator = resourceAllocator;
    this.executorServiceClientFactory = executorServiceClientFactory;
    this.jobTelemetryClient = jobTelemetryClient;
  }

  @Override
  public void start() throws Exception {
    Metrics.newGauge(Metrics.join("maestro", "active"), () -> activeQueryMap.size());

    execToCoordStatusHandlerImpl = new ExecToCoordStatusHandlerImpl();
    reader = sabotContext.get().getPlanReader();
  }

  @Override
  public void executeQuery(
    QueryId queryId,
    QueryContext context,
    PhysicalPlan physicalPlan,
    boolean runInSameThread,
    MaestroObserver observer,
    CompletionListener listener) throws ExecutionSetupException, ResourceAllocationException {

    injector.injectChecked(context.getExecutionControls(), INJECTOR_EXECUTE_QUERY_BEGIN_ERROR,
      ExecutionSetupException.class);

    // Set up the active query.
    QueryTracker queryTracker = new QueryTrackerImpl(queryId, context, physicalPlan, reader,
      resourceAllocator.get(), executorSetService.get(), executorSelectionService.get(),
      executorServiceClientFactory.get(), jobTelemetryClient.get(), observer,
      listener,
      () -> closeQuery(queryId));
    Preconditions.checkState(activeQueryMap.putIfAbsent(queryId, queryTracker) == null,
    "query already queued for execution " + QueryIdHelper.getQueryId(queryId));

    // allocate execution resources on the calling thread, as this will most likely block
    queryTracker.allocateResources();

    try {
      // do execution planning in the bound pool
      commandPool.get().submit(CommandPool.Priority.MEDIUM,
        QueryIdHelper.getQueryId(queryId) + ":execution-planning",
        (waitInMillis) -> {
          injector.injectChecked(context.getExecutionControls(),
            INJECTOR_COMMAND_POOL_SUBMIT_ERROR, ExecutionSetupException.class);

          observer.commandPoolWait(waitInMillis);
          queryTracker.planExecution();
          return null;
        }, runInSameThread).get();
    } catch (ExecutionException|InterruptedException e) {
      throw new ExecutionSetupException("failure during execution planning", e);
    }

    // propagate the fragments.
    queryTracker.startFragments();

    injector.injectChecked(context.getExecutionControls(), INJECTOR_EXECUTE_QUERY_END_ERROR,
      ExecutionSetupException.class);
  }

  @Override
  public void cancelQuery(QueryId queryId) {
    QueryTracker queryTracker = activeQueryMap.get(queryId);
    if (queryTracker == null) {
      logger.debug("Cancel request for non-existing query {}, ignoring", QueryIdHelper.getQueryId(queryId));
    } else {
      queryTracker.cancel();
    }
  }

  @Override
  public int getActiveQueryCount() {
    return activeQueryMap.size();
  }

  private void closeQuery(QueryId queryId) {
    QueryTracker queryTracker = activeQueryMap.remove(queryId);
    if (queryTracker != null) {
      // release resources held by the query.
      AutoCloseables.closeNoChecked(queryTracker);
      indicateIfSafeToExit();
    }
  }

  @Override
  public ExecToCoordStatusHandler getExecStatusHandler() {
    return execToCoordStatusHandlerImpl;
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * @return list of query Ids strings for all active execution foremen.
   */
  private List<String> getActiveQueryIds() {
    return activeQueryMap
      .keySet()
      .stream()
      .map((e) -> QueryIdHelper.getQueryId(e))
      .collect(Collectors.toList());
  }

  /**
   * Handles status messages from executors.
   */
  private class ExecToCoordStatusHandlerImpl implements ExecToCoordStatusHandler {

    @Override
    public void screenCompleted(NodeQueryScreenCompletion completion) throws RpcException {
      QueryTracker queryTracker = activeQueryMap.get(completion.getId());
      if (queryTracker == null) {
        logger.debug("screen completion message arrived post query termination, dropping. Query [{}] from node {}.",
          QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
      } else {
        queryTracker.screenCompleted(completion);
      }
    }

    @Override
    public void nodeQueryCompleted(NodeQueryCompletion completion) throws RpcException {
      QueryTracker queryTracker = activeQueryMap.get(completion.getId());
      if (queryTracker == null) {
        logger.debug("A node query completion message arrived post query termination, dropping. Query [{}] from node {}.",
          QueryIdHelper.getQueryId(completion.getId()), completion.getEndpoint());
      } else {
        queryTracker.nodeCompleted(completion);
      }
    }

    @Override
    public void nodeQueryMarkFirstError(NodeQueryFirstError error) throws RpcException {
      QueryTracker queryTracker = activeQueryMap.get(error.getHandle().getQueryId());
      if (queryTracker == null) {
        logger.debug("A node query error message arrived post query termination, dropping. Query [{}] from node {}.",
          QueryIdHelper.getQueryId(error.getHandle().getQueryId()), error.getEndpoint());
      } else {
        queryTracker.nodeMarkFirstError(error);
      }
    }
  }

  /**
   * Waits until it is safe to exit. Blocks until all currently running fragments have completed.
   *
   * <p>This is intended to be used by {@link com.dremio.exec.server.SabotNode#close()}.</p>
   */
  @Override
  public void waitToExit() {
    synchronized(this) {
      if (activeQueryMap.isEmpty()) {
        return;
      }

      exitLatch = new ExtendedLatch();
    }

    // Wait for at most 5 seconds or until the latch is released.
    exitLatch.awaitUninterruptibly(5000);
  }

  /**
   * If it is safe to exit, and the exitLatch is in use, signals it so that waitToExit() will
   * unblock.
   */
  private void indicateIfSafeToExit() {
    synchronized(this) {
      if (exitLatch != null) {
        if (activeQueryMap.isEmpty()) {
          exitLatch.countDown();
        }
      }
    }
  }


}
