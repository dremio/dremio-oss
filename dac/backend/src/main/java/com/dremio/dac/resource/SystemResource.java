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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobUI;
import com.dremio.dac.model.system.Nodes;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.server.ClusterResourceInformation;
import com.dremio.exec.server.SabotContext;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.NoOpJobStatusListener;
import com.dremio.service.jobs.SqlQuery;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Resource for system info
 */
@RestResource
@Secured
@RolesAllowed({"admin", "user"})
@Path("/system")
public class SystemResource {

  private static final Logger logger = LoggerFactory.getLogger(SystemResource.class);

  private final Provider<JobsService> jobsService;
  private final SecurityContext securityContext;
  private final Provider<SabotContext> context;

  @Inject
  public SystemResource(
    Provider<SabotContext> context,
    Provider<JobsService> jobsService,
    SecurityContext securityContext
  ) {
    this.jobsService = jobsService;
    this.securityContext = securityContext;
    this.context = context;
  }

  @GET
  @Path("/cluster-resource-info")
  @Produces(MediaType.APPLICATION_JSON)
  public ResourceInfo getClusterResourceInformation() {
    ClusterResourceInformation clusterResourceInformation =
      context.get().getClusterResourceInformation();

    ResourceInfo result;
    result = new ResourceInfo(clusterResourceInformation.getAverageExecutorMemory(),
      clusterResourceInformation.getAverageExecutorCores(context.get().getOptionManager()),
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
  public List<Nodes.NodeInfo> getNodes(){
    final List<Nodes.NodeInfo> result = new ArrayList<>();
    final Map<String, NodeEndpoint> map = new HashMap<>();

    // first get the coordinator nodes (in case there are no executors running)
    for(NodeEndpoint ep : context.get().getCoordinators()){
      map.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
      logger.info("address: " + ep.getAddress() + " fabric port: " + ep.getFabricPort());
    }

    // try to get any executor nodes, but don't throw a UserException if we can't find any
    try {
      String sql = "select\n" +
        "   'green' as status,\n" +
        "   nodes.hostname name,\n" +
        "   nodes.ip_address ip,\n" +
        "   nodes.fabric_port port,\n" +
        "   cpu cpu,\n" +
        "   memory memory \n" +
        "from\n" +
        "   sys.nodes,\n" +
        "   (select\n" +
        "      hostname,\n" +
        "      fabric_port,\n" +
        "      sum(cast(cpuTime as float) / cores) cpu \n" +
        "   from\n" +
        "      sys.threads \n" +
        "   group by\n" +
        "      hostname,\n" +
        "      fabric_port) cpu,\n" +
        "   (select\n" +
        "      hostname,\n" +
        "      fabric_port,\n" +
        "      direct_current * 100.0 / direct_max as memory \n" +
        "   from\n" +
        "      sys.memory) memory  \n" +
        "where\n" +
        "   nodes.hostname = cpu.hostname \n" +
        "   and nodes.fabric_port = cpu.fabric_port  \n" +
        "   and nodes.hostname = memory.hostname \n" +
        "   and nodes.fabric_port = memory.fabric_port \n" +
        "order by\n" +
        "   name,\n" +
        "   port";

      // TODO: Truncate the results to 500, this will change in DX-3333
      final JobDataFragment pojo = JobUI.getJobData(
        jobsService.get().submitJob(
          JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(sql, Collections.singletonList("sys"), securityContext))
            .setQueryType(QueryType.UI_INTERNAL_RUN)
            .build(),
          NoOpJobStatusListener.INSTANCE)
      ).truncate(500);

      for (NodeEndpoint ep : context.get().getExecutors()) {
        map.put(ep.getAddress() + ":" + ep.getFabricPort(), ep);
      }

      for (int i = 0; i < pojo.getReturnedRowCount(); i++) {
        String name = pojo.extractString("name", i);
        String port = pojo.extractString("port", i);
        String key = name + ":" + port;
        NodeEndpoint ep = map.remove(key);
        if (ep == null) {
          logger.warn("Unable to find node with identity: {}", key);
          continue;
        }

        boolean exec = ep.getRoles().getJavaExecutor();
        boolean coord = ep.getRoles().getSqlQuery();

        port = Integer.toString(ep.getUserPort());
        if (exec && coord) {
          name += " (c + e)";
        } else if (exec) {
          name += " (e)";
        } else {
          name += " (c)";
        }

        final Nodes.NodeInfo nodeInfo = new Nodes.NodeInfo(
          name,
          pojo.extractString("name", i),
          pojo.extractString("ip", i),
          port,
          pojo.extractString("cpu", i),
          pojo.extractString("memory", i),
          pojo.extractString("status", i),
          coord,
          exec
        );
        result.add(nodeInfo);
      }
    } catch (UserException e) {
      logger.warn(e.getMessage());
    } catch (Exception e) {
      throw e;
    }

    final List<Nodes.NodeInfo> finalList = new ArrayList<>();

    for (NodeEndpoint ep : map.values()){
      Nodes.NodeInfo nodeInfo = new Nodes.NodeInfo(
        ep.getAddress() + " (c)",
        ep.getAddress(),
        ep.getAddress(),
        Integer.toString(ep.getUserPort()),
        "0",
        "0",
        "green",
        true,
        false
      );
      finalList.add(nodeInfo);
    }

    // put coordinators first.
    finalList.addAll(result);

    return finalList;
  }
}
