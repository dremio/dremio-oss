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
    private final String port;
    private final String cpu;
    private final String memory;
    private final String status;
    private final Boolean isCoordinator;
    private final Boolean isExecutor;
    private final String nodeTag;

    @JsonCreator
    public NodeInfo(
      @JsonProperty("name") String name,
      @JsonProperty("host") String host,
      @JsonProperty("ip") String ip,
      @JsonProperty("port") String port,
      @JsonProperty("cpu") String cpu,
      @JsonProperty("memory") String memory,
      @JsonProperty("status") String status,
      @JsonProperty("isCoordinator") Boolean isCoordinator,
      @JsonProperty("isExecutor") Boolean isExecutor,
      @JsonProperty("nodeTag") String nodeTag
    ) {
      this.name = name;
      this.host = host;
      this.ip = ip;
      this.port = port;
      this.cpu = cpu;
      this.memory = memory;
      this.status = status;
      this.isCoordinator = isCoordinator;
      this.isExecutor = isExecutor;
      this.nodeTag = nodeTag;
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

    public String getPort() {
      return port;
    }

    public String getCpu() {
      return cpu;
    }

    public String getMemory() {
      return memory;
    }

    public String getStatus() {
      return status;
    }

    public Boolean getIsCoordinator() {
      return isCoordinator;
    }

    public Boolean getIsExecutor() {
      return isExecutor;
    }

    public String getNodeTag() {
      return nodeTag;
    }
  }
}
