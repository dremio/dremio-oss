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
package com.dremio.dac.resource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.SecurityContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.system.Nodes.NodeInfo;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.exec.store.sys.NodeInstance;
import com.dremio.exec.work.NodeStatsListener;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.service.executor.ExecutorServiceClientFactory;
import com.dremio.service.jobs.JobsService;
import com.dremio.services.fabric.api.FabricService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.protobuf.Empty;

/**
 * Resource for system info
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/system")
public class SystemResource extends BaseResourceWithAllocator {

  private static final Logger logger = LoggerFactory.getLogger(SystemResource.class);

  private final Provider<JobsService> jobsService;
  private final SecurityContext securityContext;
  private final Provider<SabotContext> context;
  private final Provider<FabricService> fabric;
  private final ProjectOptionManager projectOptionManager;
  private final Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider;

  @Inject
  public SystemResource (
    Provider<SabotContext> context,
    Provider<FabricService> fabric,
    Provider<JobsService> jobsService,
    Provider<ExecutorServiceClientFactory> executorServiceClientFactoryProvider,
    SecurityContext securityContext,
    BufferAllocatorFactory allocatorFactory,
    ProjectOptionManager projectOptionManager
  ) {
    super(allocatorFactory);
    this.jobsService = jobsService;
    this.securityContext = securityContext;
    this.context = context;
    this.fabric = fabric;
    this.projectOptionManager = projectOptionManager;
    this.executorServiceClientFactoryProvider = executorServiceClientFactoryProvider;
  }

  @GET
  @Path("/cluster-resource-info")
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceInfo getClusterResourceInformation() {
    GroupResourceInformation clusterResourceInformation =
      context.get().getClusterResourceInformation();

    ResourceInfo result;
    result = new ResourceInfo(clusterResourceInformation.getAverageExecutorMemory(),
      clusterResourceInformation.getAverageExecutorCores(projectOptionManager),
      clusterResourceInformation.getExecutorNodeCount());
    return result;
  }

  /**
   * MemoryInfo struct to pass back to the client
   */
  public static class ResourceInfo {
    private final Long averageExecutorMemory;
    private final Long averageExecutorCores;
    private final Integer executorCount;

    @JsonCreator
    public ResourceInfo(
      @JsonProperty("averageExecutorMemory") Long averageExecutorMemory,
      @JsonProperty("averageExecutorCores") Long averageExecutorCores,
      @JsonProperty("executorCount") Integer executorCount
    ) {
      this.averageExecutorMemory = averageExecutorMemory;
      this.averageExecutorCores = averageExecutorCores;
      this.executorCount = executorCount;
    }

    public Long getAverageExecutorMemory() {
      return averageExecutorMemory;
    }

    public Long getAverageExecutorCores() {
      return averageExecutorCores;
    }

    public Integer getExecutorCount() {
      return executorCount;
    }
  }

  @GET
  @Path("/nodes")
  @Produces(MediaType.APPLICATION_JSON)
  public List<NodeInfo> getNodes(){
    final List<NodeInfo> result = new ArrayList<>();
    final Map<String, NodeEndpoint> map = new HashMap<>();

    // first get the coordinator nodes (in case there are no executors running)
    for(NodeEndpoint ep : context.get().getCoordinators()){
      map.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
    }

    // try to get any executor nodes, but don't throw a UserException if we can't find any
    try {
      NodeStatsListener nodeStatsListener = new NodeStatsListener(context.get().getExecutors().size());
      context.get().getExecutors().forEach(
        ep -> {

          executorServiceClientFactoryProvider.get().getClientForEndpoint(ep).getNodeStats(Empty.newBuilder().build(),
                  nodeStatsListener);
        }
      );

      try {
        nodeStatsListener.waitForFinish();
      } catch (Exception ex) {
        throw UserException.connectionError(ex).message("Error while collecting node statistics")
          .build(logger);
      }

      ConcurrentHashMap<String, NodeInstance> nodeStats = nodeStatsListener.getResult();

      for (NodeEndpoint ep : context.get().getExecutors()) {
        map.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
      }

      for (Map.Entry<String, NodeInstance> statsEntry : nodeStats.entrySet()) {
        NodeInstance stat = statsEntry.getValue();
        NodeEndpoint ep = map.remove(statsEntry.getKey());
        if (ep == null) {
          logger.warn("Unable to find node with identity: {}", statsEntry.getKey());
          continue;
        }
        result.add(NodeInfo.fromNodeInstance(stat));
      }
    } catch (UserException e) {
      logger.warn(e.getMessage());
    }

    final List<NodeInfo> finalList = new ArrayList<>();
    final List<NodeInfo> coord = new ArrayList<>();
    for (NodeEndpoint ep : map.values()){
      final NodeInfo nodeInfo = NodeInfo.fromEndpoint(ep);
      if (nodeInfo.getIsMaster()) {
        finalList.add(nodeInfo);
      } else {
        coord.add(nodeInfo);
      }
    }

    // put coordinators first.
    finalList.addAll(coord);
    finalList.addAll(result);

    return finalList;
  }
}
