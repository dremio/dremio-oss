/*
 * Copyright (C) 2017 Dremio Corporation
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

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.rpc.RpcConfig;
import com.dremio.services.fabric.proto.FabricProto.FabricIdentity;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.netty.channel.EventLoopGroup;

/**
 * Manages available remote connections
 */
class ConnectionManagerRegistry implements AutoCloseable {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ConnectionManagerRegistry.class);

  private final ConcurrentMap<FabricIdentity, FabricConnectionManager> registry = Maps.newConcurrentMap();

  private volatile FabricIdentity localIdentity;
  private final BufferAllocator allocator;
  private final RpcConfig config;
  private final EventLoopGroup eventLoop;
  private final FabricMessageHandler handler;

  public ConnectionManagerRegistry(RpcConfig config, EventLoopGroup eventLoop, BufferAllocator allocator, FabricMessageHandler handler) {
    super();
    this.allocator = allocator;
    this.config = config;
    this.eventLoop = eventLoop;
    this.handler = handler;
  }

  public FabricConnectionManager getConnectionManager(FabricIdentity remoteIdentity) {
    assert localIdentity != null : "Fabric identity must be set before a connection manager can be retrieved";
    FabricConnectionManager m = registry.get(remoteIdentity);
    if (m == null) {
      m = new FabricConnectionManager(config, allocator, remoteIdentity, localIdentity, eventLoop, handler);
      FabricConnectionManager m2 = registry.putIfAbsent(remoteIdentity, m);
      if (m2 != null) {
        m = m2;
      }
    }
    return m;
  }

  public void setIdentity(FabricIdentity localIdentity) {
    this.localIdentity = localIdentity;
  }

  @Override
  public void close() throws Exception {
    List<AutoCloseable> closeables = Lists.newArrayList();

    for (FabricConnectionManager bt : registry.values()) {
      closeables.add(bt);
    }

    AutoCloseables.close(closeables);
  }

}
