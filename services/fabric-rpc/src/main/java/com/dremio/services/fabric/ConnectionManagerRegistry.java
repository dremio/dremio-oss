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
package com.dremio.services.fabric;

import static com.dremio.telemetry.api.metrics.MeterProviders.newGauge;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.dremio.ssl.SSLEngineFactory;
import com.google.common.collect.Maps;
import io.netty.channel.EventLoopGroup;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.apache.arrow.memory.BufferAllocator;

/** Manages available remote connections */
final class ConnectionManagerRegistry implements AutoCloseable {

  private final ConcurrentMap<FabricIdentity, FabricConnectionManager> registry =
      Maps.newConcurrentMap();

  private final BufferAllocator allocator;
  private final RpcConfig config;
  private final EventLoopGroup eventLoop;
  private final FabricMessageHandler handler;
  private final Optional<SSLEngineFactory> engineFactory;

  private volatile FabricIdentity localIdentity;

  ConnectionManagerRegistry(
      RpcConfig config,
      EventLoopGroup eventLoop,
      BufferAllocator allocator,
      FabricMessageHandler handler,
      Optional<SSLEngineFactory> engineFactory) {
    this.allocator = allocator;
    this.config = config;
    this.eventLoop = eventLoop;
    this.handler = handler;
    this.engineFactory = engineFactory;

    newGauge("rpc.peers", registry::size);
  }

  FabricConnectionManager getConnectionManager(FabricIdentity remoteIdentity) {
    assert localIdentity != null
        : "Fabric identity must be set before a connection manager can be retrieved";
    assert remoteIdentity != null : "Identity cannot be null.";
    assert remoteIdentity.getAddress() != null && !remoteIdentity.getAddress().isEmpty()
        : "RemoteIdentity's server address cannot be null.";
    assert remoteIdentity.getPort() > 0
        : String.format(
            "Fabric Port must be set to a port between 1 and 65k. Was set to %d.",
            remoteIdentity.getPort());

    FabricConnectionManager m = registry.get(remoteIdentity);
    if (m == null) {
      m =
          new FabricConnectionManager(
              config, allocator, remoteIdentity, localIdentity, eventLoop, handler, engineFactory);
      FabricConnectionManager m2 = registry.putIfAbsent(remoteIdentity, m);
      if (m2 != null) {
        m = m2;
      }
    }
    return m;
  }

  void setIdentity(FabricIdentity localIdentity) {
    this.localIdentity = localIdentity;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(registry.values());
  }
}
