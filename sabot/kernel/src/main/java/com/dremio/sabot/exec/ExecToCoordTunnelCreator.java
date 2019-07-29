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
package com.dremio.sabot.exec;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.google.common.base.Preconditions;

public class ExecToCoordTunnelCreator {

  private final FabricRunnerFactory factory;

  public ExecToCoordTunnelCreator(FabricRunnerFactory factory) {
    super();
    this.factory = factory;
  }

  public ExecToCoordTunnel getTunnel(NodeEndpoint identity){
    Preconditions.checkArgument(ClusterCoordinator.Role.fromEndpointRoles(identity.getRoles()).contains(ClusterCoordinator.Role.COORDINATOR), "SabotNode %s is not a coordinator node.", identity);
    return new ExecToCoordTunnel(identity, factory.getCommandRunner(identity.getAddress(), identity.getFabricPort()));

  }
}
