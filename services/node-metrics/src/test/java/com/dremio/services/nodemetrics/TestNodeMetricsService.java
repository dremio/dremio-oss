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

import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.work.NodeStatsListener;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.executor.ExecutorServiceClient;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Tests {@link NodeMetricsService} */
public class TestNodeMetricsService {
  private static final String UNRESPONSIVE_EXECUTOR_ADDRESS = "0.0.0.0";
  private static final String RESPONSIVE_EXECUTOR_ADDRESS = "0.0.0.1";
  private static final String UNRESPONSIVE_COORDINATOR_ADDRESS = "0.0.0.2";
  private static final String RESPONSIVE_COORDINATOR_ADDRESS = "0.0.0.3";

  private static final CoordinationProtos.NodeEndpoint UNRESPONSIVE_EXECUTOR =
      CoordinationProtos.NodeEndpoint.newBuilder()
          .setAddress(UNRESPONSIVE_EXECUTOR_ADDRESS)
          .setAvailableCores(1)
          .setFabricPort(1)
          .setConduitPort(1)
          .setDremioVersion(DremioVersionInfo.getVersion())
          .setRoles(CoordinationProtos.Roles.newBuilder().setJavaExecutor(true).setSqlQuery(false))
          .build();
  private static final CoordinationProtos.NodeEndpoint RESPONSIVE_EXECUTOR =
      CoordinationProtos.NodeEndpoint.newBuilder()
          .setAddress(RESPONSIVE_EXECUTOR_ADDRESS)
          .setAvailableCores(1)
          .setFabricPort(1)
          .setConduitPort(1)
          .setDremioVersion(DremioVersionInfo.getVersion())
          .setRoles(CoordinationProtos.Roles.newBuilder().setJavaExecutor(true).setSqlQuery(false))
          .build();
  private static final CoordinationProtos.NodeEndpoint UNRESPONSIVE_COORDINATOR =
      CoordinationProtos.NodeEndpoint.newBuilder()
          .setAddress(UNRESPONSIVE_COORDINATOR_ADDRESS)
          .setAvailableCores(1)
          .setFabricPort(1)
          .setConduitPort(1)
          .setDremioVersion(DremioVersionInfo.getVersion())
          .setRoles(CoordinationProtos.Roles.newBuilder().setJavaExecutor(false).setSqlQuery(true))
          .build();
  private static final CoordinationProtos.NodeEndpoint RESPONSIVE_COORDINATOR =
      CoordinationProtos.NodeEndpoint.newBuilder()
          .setAddress(RESPONSIVE_COORDINATOR_ADDRESS)
          .setAvailableCores(1)
          .setFabricPort(1)
          .setConduitPort(1)
          .setDremioVersion(DremioVersionInfo.getVersion())
          .setRoles(
              CoordinationProtos.Roles.newBuilder()
                  .setJavaExecutor(false)
                  .setSqlQuery(true)
                  .setMaster(true))
          .build();

  private static final Double EXECUTOR_MEMORY = 50.0;
  private static final Double EXECUTOR_CPU = 10.0;
  private static final int COORDINATOR_MAX_HEAP = 200;
  private static final int COORDINATOR_USED_HEAP = 50;
  private static final Double COORDINATOR_CPU = 30.0;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  @ParameterizedTest()
  @ValueSource(booleans = {false, true})
  public void getClusterMetrics(boolean collectCoordinatorUtilization) throws IOException {
    ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
    ConduitProvider conduitProvider = mock(ConduitProvider.class);

    List<CoordinationProtos.NodeEndpoint> executors =
        List.of(UNRESPONSIVE_EXECUTOR, RESPONSIVE_EXECUTOR);
    when(clusterCoordinator.getExecutorEndpoints()).thenReturn(executors);

    List<CoordinationProtos.NodeEndpoint> coordinators =
        collectCoordinatorUtilization
            ? List.of(UNRESPONSIVE_COORDINATOR, RESPONSIVE_COORDINATOR)
            : List.of(RESPONSIVE_COORDINATOR);
    when(clusterCoordinator.getCoordinatorEndpoints()).thenReturn(coordinators);

    ExecutorServiceClient unresponsiveEpExecutorServiceClient = mock(ExecutorServiceClient.class);
    doAnswer(
            invocation -> {
              NodeStatsListener nodeStatsListener = invocation.getArgument(1);
              nodeStatsListener.onError(
                  Status.DEADLINE_EXCEEDED
                      .withDescription("Simulated error for node unresponsive")
                      .asRuntimeException());
              return null;
            })
        .when(unresponsiveEpExecutorServiceClient)
        .getNodeStats(any(Empty.class), any(StreamObserver.class));

    ExecutorServiceClient responsiveEpExecutorServiceClient = mock(ExecutorServiceClient.class);
    doAnswer(
            invocation -> {
              NodeStatsListener nodeStatsListener = invocation.getArgument(1);
              nodeStatsListener.onNext(
                  CoordExecRPC.NodeStatResp.newBuilder()
                      .setEndpoint(RESPONSIVE_EXECUTOR)
                      .setNodeStats(
                          CoordExecRPC.NodeStats.newBuilder()
                              .setName(RESPONSIVE_EXECUTOR.getAddress())
                              .setIp(RESPONSIVE_EXECUTOR.getAddress())
                              .setPort(RESPONSIVE_EXECUTOR.getFabricPort())
                              .setMemory(EXECUTOR_MEMORY)
                              .setCpu(EXECUTOR_CPU)
                              .setVersion(RESPONSIVE_EXECUTOR.getDremioVersion())
                              .setStatus("green"))
                      .build());
              nodeStatsListener.onCompleted(); // called once for every node stats streamed
              return null;
            })
        .when(responsiveEpExecutorServiceClient)
        .getNodeStats(any(Empty.class), any(StreamObserver.class));

    ExecutorServiceClientFactory executorServiceClientFactory =
        new ExecutorServiceClientFactory() {
          @Override
          public ExecutorServiceClient getClientForEndpoint(
              CoordinationProtos.NodeEndpoint endpoint) {
            if (endpoint == UNRESPONSIVE_EXECUTOR) {
              return unresponsiveEpExecutorServiceClient;
            } else if (endpoint == RESPONSIVE_EXECUTOR) {
              return responsiveEpExecutorServiceClient;
            } else {
              throw new IllegalStateException("Unexpected endpoint");
            }
          }

          @Override
          public void start() {}

          @Override
          public void close() {}
        };

    if (collectCoordinatorUtilization) {
      CoordinatorMetricsServiceGrpc.CoordinatorMetricsServiceImplBase serviceImpl =
          mock(
              CoordinatorMetricsServiceGrpc.CoordinatorMetricsServiceImplBase.class,
              delegatesTo(
                  new CoordinatorMetricsServiceGrpc.CoordinatorMetricsServiceImplBase() {
                    @Override
                    public void getMetrics(
                        CoordinatorMetricsProto.GetMetricsRequest request,
                        StreamObserver<CoordinatorMetricsProto.GetMetricsResponse> respObserver) {
                      CoordinatorMetricsProto.GetMetricsResponse response =
                          CoordinatorMetricsProto.GetMetricsResponse.newBuilder()
                              .setCoordinatorMetrics(
                                  CoordinatorMetricsProto.CoordinatorMetrics.newBuilder()
                                      .setCpuUtilization(COORDINATOR_CPU)
                                      .setMaxHeapMemory(COORDINATOR_MAX_HEAP)
                                      .setUsedHeapMemory(COORDINATOR_USED_HEAP))
                              .build();
                      respObserver.onNext(response);
                      respObserver.onCompleted();
                    }
                  }));
      // Generate a unique in-process server name.
      String serverName = InProcessServerBuilder.generateName();

      // Create a server, add service, start, and register for automatic graceful shutdown.
      grpcCleanup.register(
          InProcessServerBuilder.forName(serverName)
              .directExecutor()
              .addService(serviceImpl)
              .build()
              .start());

      // Create a client channel and register for automatic graceful shutdown.
      ManagedChannel channel =
          grpcCleanup.register(
              InProcessChannelBuilder.forName(serverName).directExecutor().build());
      ManagedChannel terminatedChannel =
          grpcCleanup.register(
              InProcessChannelBuilder.forName(serverName).directExecutor().build().shutdownNow());

      when(conduitProvider.getOrCreateChannel(RESPONSIVE_COORDINATOR)).thenReturn(channel);
      when(conduitProvider.getOrCreateChannel(UNRESPONSIVE_COORDINATOR))
          .thenReturn(terminatedChannel);
    }

    NodeMetricsService nodeMetricsService =
        new NodeMetricsService(
            () -> clusterCoordinator, () -> conduitProvider, () -> executorServiceClientFactory);

    List<NodeMetrics> nodes = nodeMetricsService.getClusterMetrics(collectCoordinatorUtilization);

    AtomicInteger nodeIdx = new AtomicInteger();

    double coordinatorCpuUtilization = collectCoordinatorUtilization ? COORDINATOR_CPU : 0;
    double coordinatorMemoryUtilization =
        collectCoordinatorUtilization ? 100.0 * COORDINATOR_USED_HEAP / COORDINATOR_MAX_HEAP : 0;
    Assertions.assertTrue(
        () -> {
          NodeMetrics node = nodes.get(nodeIdx.getAndIncrement());
          return RESPONSIVE_COORDINATOR_ADDRESS.equals(node.getIp())
              && "green".equals(node.getStatus())
              && coordinatorCpuUtilization == node.getCpu()
              && coordinatorMemoryUtilization == node.getMemory()
              && node.getDetails() == null;
        });

    Assertions.assertTrue(
        () -> {
          NodeMetrics node = nodes.get(nodeIdx.getAndIncrement());
          return RESPONSIVE_EXECUTOR_ADDRESS.equals(node.getIp())
              && "green".equals(node.getStatus())
              && EXECUTOR_CPU.equals(node.getCpu())
              && EXECUTOR_MEMORY.equals(node.getMemory())
              && node.getDetails() == null;
        });

    if (collectCoordinatorUtilization) {
      Assertions.assertTrue(
          () -> {
            NodeMetrics node = nodes.get(nodeIdx.getAndIncrement());
            return UNRESPONSIVE_COORDINATOR_ADDRESS.equals(node.getIp())
                && "red".equals(node.getStatus())
                && null == node.getCpu()
                && null == node.getMemory()
                && node.getDetails().contains(NodeState.NO_RESPONSE.getValue());
          });
    }

    Assertions.assertTrue(
        () -> {
          NodeMetrics node = nodes.get(nodeIdx.getAndIncrement());
          return UNRESPONSIVE_EXECUTOR_ADDRESS.equals(node.getIp())
              && "red".equals(node.getStatus())
              && null == node.getCpu()
              && null == node.getMemory()
              && node.getDetails().contains(NodeState.NO_RESPONSE.getValue());
        });
  }
}
