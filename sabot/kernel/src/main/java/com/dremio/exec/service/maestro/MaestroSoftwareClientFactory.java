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
package com.dremio.exec.service.maestro;

import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.sabot.exec.ExecToCoordTunnelCreator;
import com.dremio.service.maestroservice.MaestroClient;
import com.dremio.service.maestroservice.MaestroClientFactory;


/**
 * Software (i.e. on-prem product) version of the client factory.
 */
public class MaestroSoftwareClientFactory implements MaestroClientFactory {
  private final ExecToCoordTunnelCreator tunnelCreator;

  public MaestroSoftwareClientFactory(ExecToCoordTunnelCreator tunnelCreator) {
    this.tunnelCreator = tunnelCreator;
  }

  @Override
  public MaestroClient getMaestroClient(CoordinationProtos.NodeEndpoint endpoint) {
    return new MaestroSoftwareClient(tunnelCreator.getTunnel(endpoint));
  }

  @Override
  public void start() throws Exception {

  }

  @Override
  public void close() throws Exception {

  }
}
