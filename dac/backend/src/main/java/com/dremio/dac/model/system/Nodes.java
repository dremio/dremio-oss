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
package com.dremio.dac.model.system;

import java.util.ArrayList;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.api.JsonISODateTime;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.sys.NodeInstance;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Wrapper on top of List<Node> to use in testing
 */
public class Nodes extends ArrayList<Nodes.NodeInfo> {

  /**
   * Node info
   */
  public static class NodeInfo {
    private final String name;
    private final String host;
    private final String ip;
    private final Integer port;
    private final Double cpu;
    private final Double memory;
    private final String status;
    private final Boolean isMaster;
    private final Boolean isCoordinator;
    private final Boolean isExecutor;
    private final String nodeTag;
    private final String version;
    private final long start;

    @JsonCreator
    public NodeInfo(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("ip") String ip,
      @JsonProperty("port") Integer port,
      @JsonProperty("cpu") Double cpu,
      @JsonProperty("memory") Double memory,
      @JsonProperty("status") String status,
      @JsonProperty("isMaster") Boolean isMaster,
      @JsonProperty("isCoordinator") Boolean isCoordinator,
      @JsonProperty("isExecutor") Boolean isExecutor,
      @JsonProperty("nodeTag") String nodeTag,
      @JsonProperty("version") String version,
      @JsonISODateTime
      @JsonProperty("start") long start
    ) {
      this.name = name;
      this.host = host;
      this.ip = ip;
      this.port = port;
      this.cpu = cpu;
      this.memory = memory;
      this.status = status;
      this.isMaster = isMaster;
      this.isCoordinator = isCoordinator;
      this.isExecutor = isExecutor;
      this.nodeTag = nodeTag;
      this.version = version;
      this.start = start;
    }

    public static NodeInfo fromNodeInstance(NodeInstance nodeInstance) {
      return new NodeInfo(
        nodeInstance.name,
        nodeInstance.hostname,
        nodeInstance.ip,
        nodeInstance.user_port,
        nodeInstance.cpu,
        nodeInstance.memory,
        nodeInstance.status,
        nodeInstance.is_master,
        nodeInstance.is_coordinator,
        nodeInstance.is_executor,
        nodeInstance.node_tag,
        nodeInstance.version,
        nodeInstance.start.getMillis());
    }

    public static NodeInfo fromEndpoint(CoordinationProtos.NodeEndpoint endpoint) {
      final boolean master = endpoint.getRoles().getMaster();
      final boolean coord = endpoint.getRoles().getSqlQuery();
      final boolean exec = endpoint.getRoles().getJavaExecutor();
      return new NodeInfo(
        endpoint.getAddress(),
        endpoint.getAddress(),
        endpoint.getAddress(),
        endpoint.getUserPort(),
        0d,
        0d,
        "green",
        master,
        coord,
        exec,
        endpoint.getNodeTag(),
        DremioVersionInfo.getVersion(),
        endpoint.getStartTime()
      );
    }

    public String getName() {
      return name;
    }

    public String getHost() {
      return host;
    }

    public String getIp() {
      return ip;
    }

    public Integer getPort() {
      return port;
    }

    public Double getCpu() {
      return cpu;
    }

    public Double getMemory() {
      return memory;
    }

    public String getStatus() {
      return status;
    }

    public Boolean getIsMaster() { return isMaster; }

    public Boolean getIsCoordinator() {
      return isCoordinator;
    }

    public Boolean getIsExecutor() {
      return isExecutor;
    }

    public String getNodeTag() {
      return nodeTag;
    }

    public String getVersion() {
      return version;
    }

    public long getStart() {
      return start;
    }
  }
}
