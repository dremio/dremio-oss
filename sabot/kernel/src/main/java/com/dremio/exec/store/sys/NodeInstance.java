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
package com.dremio.exec.store.sys;

import org.joda.time.DateTime;

import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * Node info
 */
public class NodeInstance {

  public final String name;
  public final String hostname;
  public final String ip;
  public final Integer user_port;
  public final Integer fabric_port;
  public final Double cpu;
  public final Double memory;
  public final String status;
  public final Boolean is_coordinator;
  public final Boolean is_executor;
  public final String node_tag;
  public final String version;
  public final Double load;
  public final Integer configured_max_width;
  public final Integer actual_max_width;
  public final Boolean current;
  public final DateTime start;

  @JsonCreator
  public NodeInstance(
    String name,
    String hostname,
    String ip,
    Integer user_port,
    Integer fabric_port,
    Double cpu,
    Double memory,
    String status,
    Boolean is_coordinator,
    Boolean is_executor,
    String node_tag,
    String version,
    DateTime start,
    Double load,
    Integer configured_max_width,
    Integer actual_max_width,
    Boolean current
  ) {
    this.name = name;
    this.hostname = hostname;
    this.ip = ip;
    this.user_port = user_port;
    this.fabric_port = fabric_port;
    this.cpu = cpu;
    this.memory = memory;
    this.status = status;
    this.is_coordinator = is_coordinator;
    this.is_executor = is_executor;
    this.node_tag = node_tag;
    this.version = version;
    this.start = start;
    this.load = load;
    this.configured_max_width = configured_max_width;
    this.actual_max_width = actual_max_width;
    this.current = current;
  }

  public static NodeInstance fromStats(CoordExecRPC.NodeStats nodeStats, CoordinationProtos.NodeEndpoint ep) {
    boolean exec = ep.getRoles().getJavaExecutor();
    boolean coord = ep.getRoles().getSqlQuery();
    String name = nodeStats.getName();
    if (exec && coord) {
      name += " (c + e)";
    } else if (exec) {
      name += " (e)";
    } else {
      name += " (c)";
    }
    return new NodeInstance(
      name,
      nodeStats.getName(),
      nodeStats.getIp(),
      ep.getUserPort(),
      ep.getFabricPort(),
      nodeStats.getCpu(),
      nodeStats.getMemory(),
      nodeStats.getStatus(),
      coord,
      exec,
      ep.getNodeTag(),
      nodeStats.getVersion(),
      new DateTime(ep.getStartTime()),
      nodeStats.getLoad(),
      nodeStats.getConfiguredMaxWidth(),
      nodeStats.getActualMaxWith(),
      nodeStats.getCurrent());
  }
}
