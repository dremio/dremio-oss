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
package com.dremio.exec.work.rpc;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.sabot.rpc.Protocols;
import com.dremio.services.fabric.api.FabricService;
import javax.inject.Inject;
import javax.inject.Provider;

public class CoordToExecTunnelCreator {

  private final Provider<FabricService> factory;

  @Inject
  public CoordToExecTunnelCreator(Provider<FabricService> factory) {
    super();
    this.factory = factory;
  }

  public CoordToExecTunnel getTunnel(NodeEndpoint ep) {
    return new CoordToExecTunnel(
        ep,
        factory
            .get()
            .getProtocol(Protocols.COORD_TO_EXEC)
            .getCommandRunner(ep.getAddress(), ep.getFabricPort()));
  }
}
