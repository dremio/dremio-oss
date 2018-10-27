/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.services.fabric;

import java.util.Optional;
import java.util.concurrent.Executor;

import com.dremio.exec.rpc.RpcConfig;
import com.dremio.exec.rpc.ssl.SSLConfig;
import com.dremio.services.fabric.proto.FabricProto.FabricHandshake;
import com.dremio.services.fabric.proto.FabricProto.FabricMessage;
import com.dremio.services.fabric.proto.FabricProto.RpcType;

/**
 * Describes the wire level protocol for the Fabric.
 */
class FabricRpcConfig {

  public static RpcConfig getMapping(int rpcTimeout, Executor executor, Optional<SSLConfig> sslConfig) {
    return RpcConfig.newBuilder()
        .name("FABRIC")
        .executor(executor)
        .timeout(rpcTimeout)
        .sslConfig(sslConfig)
        .add(RpcType.HANDSHAKE, FabricHandshake.class, RpcType.HANDSHAKE, FabricHandshake.class)
        .add(RpcType.MESSAGE, FabricMessage.class, RpcType.MESSAGE, FabricMessage.class)
        .build();
  }

  public static final int RPC_VERSION = 1;
}
