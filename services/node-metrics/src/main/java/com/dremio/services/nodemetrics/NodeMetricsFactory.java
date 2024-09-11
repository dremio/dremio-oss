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

import static com.dremio.common.util.DremioVersionUtils.isCompatibleVersion;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.sys.NodeInstance;

/** Factory to create a NodeMetrics instance */
public final class NodeMetricsFactory {
  private NodeMetricsFactory() {}

  public static NodeMetrics newNodeMetrics(NodeInstance nodeInstance) {
    boolean isCompatible = isCompatibleVersion(nodeInstance.version);
    return new ImmutableNodeMetrics.Builder()
        .setName(nodeInstance.name)
        .setHost(nodeInstance.hostname)
        .setIp(nodeInstance.ip)
        .setPort(nodeInstance.user_port)
        .setCpu(nodeInstance.cpu)
        .setMemory(nodeInstance.memory)
        .setStatus(nodeInstance.status)
        .setIsMaster(nodeInstance.is_master)
        .setIsCoordinator(nodeInstance.is_coordinator)
        .setIsExecutor(nodeInstance.is_executor)
        .setIsCompatible(isCompatible)
        .setNodeTag(nodeInstance.node_tag)
        .setVersion(nodeInstance.version)
        .setStart(nodeInstance.start.getMillis())
        .setDetails(
            isCompatible
                ? NodeState.NONE.toMessage(null)
                : NodeState.INVALID_VERSION.toMessage(nodeInstance.version))
        .build();
  }

  /**
   * @param endpoint node info
   * @param availableNodeStatus status info; null expected if node is not accessible
   */
  public static NodeMetrics newNodeMetrics(
      CoordinationProtos.NodeEndpoint endpoint, AvailableNodeStatus availableNodeStatus) {
    boolean master = endpoint.getRoles().getMaster();
    boolean coord = endpoint.getRoles().getSqlQuery();
    boolean exec = endpoint.getRoles().getJavaExecutor();
    boolean isCompatible = isCompatibleVersion(endpoint.getDremioVersion());
    ImmutableNodeMetrics.Builder builder = new ImmutableNodeMetrics.Builder();
    if (availableNodeStatus != null) {
      builder.setCpu(availableNodeStatus.cpuUtilizationPercent);
      builder.setMemory(availableNodeStatus.memoryUtilizationPercent);
      builder.setStatus("green");
      builder.setDetails(
          isCompatible
              ? NodeState.NONE.toMessage(null)
              : NodeState.INVALID_VERSION.toMessage(endpoint.getDremioVersion()));
    } else {
      builder.setStatus("red");
      builder.setDetails(NodeState.NO_RESPONSE.toMessage(null));
    }
    return builder
        .setName(endpoint.getAddress())
        .setHost(endpoint.getAddress())
        .setIp(endpoint.getAddress())
        .setPort(endpoint.getUserPort())
        .setIsMaster(master)
        .setIsCoordinator(coord)
        .setIsExecutor(exec)
        .setIsCompatible(isCompatible)
        .setNodeTag(endpoint.getNodeTag())
        .setVersion(endpoint.getDremioVersion())
        .setStart(endpoint.getStartTime())
        .build();
  }

  /** Status information about an accessible node */
  public static class AvailableNodeStatus {
    private final double cpuUtilizationPercent;
    private final double memoryUtilizationPercent;

    public AvailableNodeStatus(double cpuUtilizationPercent, double memoryUtilizationPercent) {
      this.cpuUtilizationPercent = cpuUtilizationPercent;
      this.memoryUtilizationPercent = memoryUtilizationPercent;
    }
  }
}
