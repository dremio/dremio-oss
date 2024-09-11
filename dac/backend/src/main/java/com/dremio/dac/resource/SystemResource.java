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

import com.dremio.dac.annotations.RestResource;
import com.dremio.dac.annotations.Secured;
import com.dremio.dac.model.system.NodeInfo;
import com.dremio.dac.server.BufferAllocatorFactory;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.ProjectOptionManager;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.services.nodemetrics.NodeMetrics;
import com.dremio.services.nodemetrics.NodeMetricsService;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Resource for system info */
@RestResource
@Secured
@RolesAllowed({"admin"})
@Path("/system")
public class SystemResource extends BaseResourceWithAllocator {

  private static final Logger logger = LoggerFactory.getLogger(SystemResource.class);

  private final Provider<SabotContext> context;
  private final ProjectOptionManager projectOptionManager;
  private final NodeMetricsService nodeMetricsService;

  @Inject
  public SystemResource(
      Provider<SabotContext> context,
      BufferAllocatorFactory allocatorFactory,
      ProjectOptionManager projectOptionManager,
      NodeMetricsService nodeMetricsService) {
    super(allocatorFactory);
    this.context = context;
    this.projectOptionManager = projectOptionManager;
    this.nodeMetricsService = nodeMetricsService;
  }

  @GET
  @Path("/cluster-resource-info")
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceInfo getClusterResourceInformation() {
    GroupResourceInformation clusterResourceInformation =
        context.get().getClusterResourceInformation();

    ResourceInfo result;
    result =
        new ResourceInfo(
            clusterResourceInformation.getAverageExecutorMemory(),
            clusterResourceInformation.getAverageExecutorCores(projectOptionManager),
            clusterResourceInformation.getExecutorNodeCount());
    return result;
  }

  /** MemoryInfo struct to pass back to the client */
  public static class ResourceInfo {
    private final Long averageExecutorMemory;
    private final Long averageExecutorCores;
    private final Integer executorCount;

    @JsonCreator
    public ResourceInfo(
        @JsonProperty("averageExecutorMemory") Long averageExecutorMemory,
        @JsonProperty("averageExecutorCores") Long averageExecutorCores,
        @JsonProperty("executorCount") Integer executorCount) {
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
  public List<NodeInfo> getNodes() {
    List<NodeMetrics> nodeMetricsList = nodeMetricsService.getClusterMetrics(false);
    return nodeMetricsList.stream().map(NodeInfo::of).collect(Collectors.toList());
  }
}
