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
package com.dremio.services.fabric.api;

import com.dremio.service.Service;

/** Service to register custom protocols on the RPC fabric. */
public interface FabricService extends Service {

  /**
   * Register a new protocol.
   *
   * @param protocol The protocol to register.
   * @return A way to get tunnels to other nodes.
   */
  FabricRunnerFactory registerProtocol(FabricProtocol protocol);

  /**
   * Returns a runner factory for the provided protocol id
   *
   * @param id the protocol id. Has to be already registered
   * @return a way to get tunnels to other nodes
   * @throws IllegalArgumentException if the protocol hasn't been registered
   */
  FabricRunnerFactory getProtocol(int id);

  /**
   * The port the service is running on (once started).
   *
   * @return A port number.
   */
  int getPort();

  /**
   * Return the address that the rpc layer is running on.
   *
   * @return The bind address.
   */
  String getAddress();
}
