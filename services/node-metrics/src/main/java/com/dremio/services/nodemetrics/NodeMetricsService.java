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
package com.dremio.services.nodemetrics;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.sys.NodeInstance;
import com.dremio.exec.work.NodeStatsListener;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retrieves metrics about cluster coordinators and executors */
public class NodeMetricsService {
  private static final Logger logger = LoggerFactory.getLogger(NodeMetricsService.class);
  private static final int GRPC_TIMEOUT_IN_SECONDS = 5;

  private final Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider;
  private final Provider<ClusterCoordinator> clusterCoordinatorProvider;
  private final Provider<ConduitProvider> conduitProviderProvider;

  public NodeMetricsService(
      Provider<ClusterCoordinator> clusterCoordinatorProvider,
      Provider<ConduitProvider> conduitProviderProvider,
      Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider) {
    this.clusterCoordinatorProvider = clusterCoordinatorProvider;
    this.conduitProviderProvider = conduitProviderProvider;
    this.executorServiceClientFactoryProvider = executorServiceClientFactoryProvider;
  }

  public List<NodeMetrics> getClusterMetrics(boolean collectCoordinatorUtilization) {
    List<NodeMetrics> executorsMetrics = new ArrayList<>();
    Map<String, CoordinationProtos.NodeEndpoint> executorsByAddress = new HashMap<>();
    Map<String, CoordinationProtos.NodeEndpoint> coordinatorsByAddress = new HashMap<>();

    ClusterCoordinator clusterCoordinator = clusterCoordinatorProvider.get();

    // first get the coordinator nodes
    for (CoordinationProtos.NodeEndpoint ep : clusterCoordinator.getCoordinatorEndpoints()) {
      coordinatorsByAddress.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
    }

    // try to get any executor nodes, but don't throw an exception if we can't find any
    try {
      Collection<CoordinationProtos.NodeEndpoint> executorEndpoints =
          clusterCoordinator.getExecutorEndpoints();
      NodeStatsListener nodeStatsListener = new NodeStatsListener(executorEndpoints.size());
      executorEndpoints.forEach(
          ep -> {
            executorServiceClientFactoryProvider
                .get()
                .getClientForEndpoint(ep)
                .getNodeStats(Empty.newBuilder().build(), nodeStatsListener);
          });

      try {
        nodeStatsListener.waitForFinish();
      } catch (Exception ex) {
        logger.warn("Error while collecting node statistics", ex);
      }

      ConcurrentHashMap<String, NodeInstance> nodeStats = nodeStatsListener.getResult();

      for (CoordinationProtos.NodeEndpoint ep : executorEndpoints) {
        executorsByAddress.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
      }

      for (Map.Entry<String, NodeInstance> statsEntry : nodeStats.entrySet()) {
        NodeInstance stat = statsEntry.getValue();
        CoordinationProtos.NodeEndpoint executor = executorsByAddress.remove(statsEntry.getKey());
        coordinatorsByAddress.remove(statsEntry.getKey());
        if (executor == null) {
          logger.warn("Unable to find node with identity: {}", statsEntry.getKey());
          continue;
        }
        executorsMetrics.add(NodeMetricsFactory.newNodeMetrics(stat));
      }
    } catch (UserException e) {
      logger.warn("Error when resolving metrics for executors", e);
    }

    List<NodeMetrics> allNodesMetrics = new ArrayList<>();
    List<NodeMetrics> coordinatorsMetrics = new ArrayList<>();
    List<CoordinationProtos.NodeEndpoint> unresponsiveCoordinators = new ArrayList<>();
    for (CoordinationProtos.NodeEndpoint coordinator : coordinatorsByAddress.values()) {
      double cpuUtilization = 0;
      double memoryUtilization = 0;
      if (collectCoordinatorUtilization) {
        try {
          CoordinatorMetricsProto.GetMetricsResponse getMetricsResponse =
              getCoordinatorMetrics(coordinator);
          CoordinatorMetricsProto.CoordinatorMetrics coordinatorMetrics =
              getMetricsResponse.getCoordinatorMetrics();
          logger.debug("Coordinator metrics: {}", coordinatorMetrics);
          cpuUtilization = coordinatorMetrics.getCpuUtilization();
          memoryUtilization =
              coordinatorMetrics.hasMaxHeapMemory()
                  ? coordinatorMetrics.getUsedHeapMemory()
                      * 100.0
                      / coordinatorMetrics.getMaxHeapMemory()
                  : 0;
        } catch (ExecutionException
            | InterruptedException
            | TimeoutException
            | RuntimeException e) {
          // We don't propagate this exception so that other nodes' metrics are returned and the
          // unresponsive coordinators are marked as such
          logger.warn("Error while collecting coordinator metrics", e);
          unresponsiveCoordinators.add(coordinator);
          continue;
        }
      }
      NodeMetrics nodeMetrics =
          NodeMetricsFactory.newNodeMetrics(
              coordinator,
              new NodeMetricsFactory.AvailableNodeStatus(cpuUtilization, memoryUtilization));
      if (nodeMetrics.getIsMaster()) {
        coordinatorsMetrics.add(0, nodeMetrics);
      } else {
        coordinatorsMetrics.add(nodeMetrics);
      }
    }

    // responsive executors were already removed from the map
    Collection<CoordinationProtos.NodeEndpoint> unresponsiveExecutors = executorsByAddress.values();
    List<NodeMetrics> failedNodesMetrics =
        Stream.of(unresponsiveCoordinators, unresponsiveExecutors)
            .flatMap(Collection::stream)
            .map(ep -> NodeMetricsFactory.newNodeMetrics(ep, null))
            .collect(Collectors.toList());

    // put coordinators first.
    allNodesMetrics.addAll(coordinatorsMetrics);
    allNodesMetrics.addAll(executorsMetrics);
    allNodesMetrics.addAll(failedNodesMetrics);

    return allNodesMetrics;
  }

  private CoordinatorMetricsProto.GetMetricsResponse getCoordinatorMetrics(
      CoordinationProtos.NodeEndpoint coordinator)
      throws ExecutionException, InterruptedException, TimeoutException {
    // Throws RuntimeException if it cannot create a channel to the given coordinator
    ManagedChannel channel = conduitProviderProvider.get().getOrCreateChannel(coordinator);
    CoordinatorMetricsServiceGrpc.CoordinatorMetricsServiceBlockingStub blockingStub =
        CoordinatorMetricsServiceGrpc.newBlockingStub(channel);
    return blockingStub
        .withDeadlineAfter(GRPC_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS)
        .getMetrics(CoordinatorMetricsProto.GetMetricsRequest.getDefaultInstance());
  }
}
