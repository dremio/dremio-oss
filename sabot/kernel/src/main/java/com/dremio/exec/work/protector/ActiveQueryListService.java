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

package com.dremio.exec.work.protector;

import com.dremio.common.nodes.EndpointHelper;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.maestro.MaestroService;
import com.dremio.exec.proto.CoordExecRPC.ActiveQueriesOnForeman;
import com.dremio.exec.proto.CoordExecRPC.ActiveQueryList;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.AdminBooleanValidator;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.service.Service;
import com.dremio.service.activequerylistservice.ActiveQueryListServiceGrpc;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.ExecutorSetService;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service to determine active queries (aka Active Query List) on coordinator side and send it
 * across to executor
 */
public class ActiveQueryListService
    extends ActiveQueryListServiceGrpc.ActiveQueryListServiceImplBase implements Service {
  private static final Logger logger = LoggerFactory.getLogger(ActiveQueryListService.class);
  private static final String LOCAL_TASK_LEADER_NAME = "activeQueryListService";
  private static final long ACTIVE_QUERY_LIST_LEADERSHIP_RELEASE_SECS = 24 * 60 * 60;
  private static final AdminBooleanValidator enableReconcileQueriesOption =
      ExecConstants.ENABLE_RECONCILE_QUERIES;
  private static final LongValidator reconcileQueriesFreqSecsOption =
      ExecConstants.RECONCILE_QUERIES_FREQUENCY_SECS;

  private Provider<SchedulerService> schedulerServiceProvider;
  private Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider;
  private Provider<NodeEndpoint> nodeEndpointProvider;
  private Provider<ExecutorSetService> executorSetServiceProvider;
  private Provider<MaestroService> maestroServiceProvider;
  private Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private Provider<ConduitProvider> conduitProviderProvider;
  private Provider<OptionManager> optionManagerProvider;
  private boolean isDistributedMaster;

  private Cancellable activeQueryListTask;

  public ActiveQueryListService(
      Provider<SchedulerService> schedulerServiceProvider,
      Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider,
      Provider<NodeEndpoint> nodeEndpointProvider,
      Provider<ExecutorSetService> executorSetServiceProvider,
      Provider<MaestroService> maestroServiceProvider,
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<ConduitProvider> conduitProviderProvider,
      Provider<OptionManager> optionManagerProvider,
      boolean isDistributedMaster) {
    this.schedulerServiceProvider = schedulerServiceProvider;
    this.executorServiceClientFactoryProvider = executorServiceClientFactoryProvider;
    this.nodeEndpointProvider = nodeEndpointProvider;
    this.executorSetServiceProvider = executorSetServiceProvider;
    this.maestroServiceProvider = maestroServiceProvider;
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.conduitProviderProvider = conduitProviderProvider;
    this.optionManagerProvider = optionManagerProvider;
    this.isDistributedMaster = isDistributedMaster;
  }

  @Override
  public void start() throws Exception {
    if (isDistributedMaster) {
      OptionManager optionManager = optionManagerProvider.get();
      startReconcilingQueries(
          optionManager.getOption(enableReconcileQueriesOption),
          optionManager.getOption(reconcileQueriesFreqSecsOption));
      optionManager.addOptionChangeListener(new AQLOptionChangeListener(optionManager));
    }
  }

  private void startReconcilingQueries(
      boolean enableReconcileQueries, long reconcileQueriesFreqSecs) {
    if (enableReconcileQueries) {
      Schedule schedule =
          Schedule.Builder.everySeconds(reconcileQueriesFreqSecs)
              .asClusteredSingleton(LOCAL_TASK_LEADER_NAME)
              .releaseOwnershipAfter(ACTIVE_QUERY_LIST_LEADERSHIP_RELEASE_SECS, TimeUnit.SECONDS)
              .build();
      activeQueryListTask =
          schedulerServiceProvider.get().schedule(schedule, createActiveQueryListTask());
      logger.info("Starting reconciling active queries");
    }
  }

  @Override
  public void close() throws Exception {
    logger.info("Stopping ActiveQueryListService");
    stopReconcilingQueries();
    logger.info("Stopped ActiveQueryListService");
  }

  private void stopReconcilingQueries() {
    if (activeQueryListTask != null) {
      activeQueryListTask.cancel(true);
      activeQueryListTask = null;
      logger.info("Stopped reconciling active queries");
    }
  }

  /**
   * Listen to option changes. Determine whether options related reconcile queries are changed.
   * Accordingly start/stop/reconfigure the schedule of sending ActiveQueryList.
   */
  private class AQLOptionChangeListener implements OptionChangeListener {
    private boolean enableReconcileQueries;
    private long reconcileQueriesFreqSecs;
    private OptionManager optionManager;

    public AQLOptionChangeListener(OptionManager optionManager) {
      this.optionManager = optionManager;
      this.enableReconcileQueries = optionManager.getOption(enableReconcileQueriesOption);
      this.reconcileQueriesFreqSecs = optionManager.getOption(reconcileQueriesFreqSecsOption);
    }

    @Override
    public synchronized void onChange() {
      boolean newEnableReconcileQueries = optionManager.getOption(enableReconcileQueriesOption);
      long newReconcileQueriesFreqSecs = optionManager.getOption(reconcileQueriesFreqSecsOption);
      if (newEnableReconcileQueries != enableReconcileQueries
          || newReconcileQueriesFreqSecs != reconcileQueriesFreqSecs) {
        logger.info("Options related to reconcile active queries changed.");
        stopReconcilingQueries();
        enableReconcileQueries = newEnableReconcileQueries;
        reconcileQueriesFreqSecs = newReconcileQueriesFreqSecs;
        startReconcilingQueries(enableReconcileQueries, reconcileQueriesFreqSecs);
      }
    }
  }

  /**
   * Determine active queries (aka Active Query List) on coordinator side and send it across to
   * executor.
   */
  class ActiveQueryListTask implements Runnable {

    @Override
    public void run() {
      logger.info("Starting activeQueryListTask on this coordinator.");

      ExecutorSetService executorSetService = executorSetServiceProvider.get();
      if (executorSetService == null) {
        logger.debug("Seems executorSetService is not yet initialized.");
        return;
      }

      ActiveQueryList.Builder activeQueryListBuilder = ActiveQueryList.newBuilder();

      try {
        // Add active queries from local coordinator to ActiveQueryList
        ActiveQueriesOnForeman activeQueriesOnForeman = getActiveQueries();
        activeQueryListBuilder.addActiveQueriesOnForeman(activeQueriesOnForeman);

        final NodeEndpoint localCoordinator =
            EndpointHelper.getMinimalEndpoint(nodeEndpointProvider.get());
        Collection<NodeEndpoint> remoteCoordinators =
            clusterCoordinatorProvider.get().getCoordinatorEndpoints().stream()
                .filter(x -> !localCoordinator.equals(EndpointHelper.getMinimalEndpoint(x)))
                .collect(Collectors.toList());

        // Add active queries from remote coordinators to ActiveQueryList
        for (NodeEndpoint remoteCoordinator : remoteCoordinators) {
          activeQueryListBuilder.addActiveQueriesOnForeman(
              getActiveQueriesOnRemoteCoordinator(remoteCoordinator));
        }
      } catch (Exception e) {
        logger.error(
            "Exception during determining active queries across all coordinators. "
                + "Not sending active queries to executors for reconciling.",
            e);
        return;
      }

      ActiveQueryList activeQueryList = activeQueryListBuilder.build();
      Collection<NodeEndpoint> allExecutorNodeEndpoints =
          executorSetService.getAllAvailableEndpoints();

      for (NodeEndpoint executor : allExecutorNodeEndpoints) {
        logger.debug(
            "Sending activeQueryList to executor:{}", EndpointHelper.getMinimalString(executor));

        try {
          executorServiceClientFactoryProvider
              .get()
              .getClientForEndpoint(executor)
              .reconcileActiveQueries(
                  activeQueryList, new ActiveQueryListResponseObserver(executor));
        } catch (Exception e) {
          logger.error(
              "Error sending active queries to executor:{}",
              EndpointHelper.getMinimalString(executor),
              e);
        }
      }
    }

    private ActiveQueriesOnForeman getActiveQueriesOnRemoteCoordinator(
        NodeEndpoint remoteCoordinator) {
      logger.info(
          "Fetching activeQueries from remoteCoordinator:{}",
          EndpointHelper.getMinimalString(remoteCoordinator));
      final ManagedChannel channel =
          conduitProviderProvider.get().getOrCreateChannel(remoteCoordinator);
      return ActiveQueryListServiceGrpc.newBlockingStub(channel)
          .getActiveQueriesOnForeman(Empty.getDefaultInstance());
    }
  }

  class ActiveQueryListResponseObserver implements StreamObserver<Empty> {
    private final NodeEndpoint executor;

    public ActiveQueryListResponseObserver(NodeEndpoint executor) {
      this.executor = executor;
    }

    @Override
    public void onNext(Empty value) {}

    @Override
    public void onError(Throwable t) {
      logger.error(
          "Received error from executor: {}", EndpointHelper.getMinimalEndpoint(executor), t);
    }

    @Override
    public void onCompleted() {
      logger.debug(
          "Received onCompleted from executor: {}", EndpointHelper.getMinimalEndpoint(executor));
    }
  }

  @VisibleForTesting
  public ActiveQueryListTask createActiveQueryListTask() {
    return new ActiveQueryListTask();
  }

  public ActiveQueriesOnForeman getActiveQueries() {
    NodeEndpoint localCoordinator = EndpointHelper.getMinimalEndpoint(nodeEndpointProvider.get());
    List<QueryId> queryIdList = maestroServiceProvider.get().getActiveQueryIds();

    ActiveQueriesOnForeman activeQueriesOnForeman =
        ActiveQueriesOnForeman.newBuilder()
            .setForeman(localCoordinator)
            .addAllQueryId(queryIdList)
            .setTimestamp(System.currentTimeMillis())
            .build();
    return activeQueriesOnForeman;
  }

  /**
   * This will be called from other coordinators.
   *
   * @param request
   * @param responseObserver
   */
  @Override
  public void getActiveQueriesOnForeman(
      com.google.protobuf.Empty request,
      io.grpc.stub.StreamObserver<ActiveQueriesOnForeman> responseObserver) {

    ActiveQueriesOnForeman activeQueriesOnForeman;
    try {
      activeQueriesOnForeman = getActiveQueries();
    } catch (Exception e) {
      responseObserver.onError(
          Status.INTERNAL
              .withDescription(
                  "Failed getting active queries on foreman: "
                      + EndpointHelper.getMinimalString(nodeEndpointProvider.get())
                      + ", exception:"
                      + e.getMessage())
              .asRuntimeException());
      return;
    }
    responseObserver.onNext(activeQueriesOnForeman);
    responseObserver.onCompleted();
  }
}
