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
package com.dremio.exec.store.dfs;

import java.util.concurrent.ExecutorService;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.services.fabric.api.FabricRunnerFactory;

/**
 * Class used to pass configuration to file system instances.
 */
class PDFSConfig {

  private final ExecutorService executor;
  private final FabricRunnerFactory runnerFactory;
  private final BufferAllocator allocator;
  private final Provider<Iterable<NodeEndpoint>> endpointProvider;
  private final NodeEndpoint localIdentity;
  private final boolean localAccessAllowed;

  public PDFSConfig(ExecutorService executor, FabricRunnerFactory runnerFactory, BufferAllocator allocator,
      Provider<Iterable<NodeEndpoint>> endpointProvider, NodeEndpoint localIdentity, boolean localAccessAllowed) {
    super();
    this.executor = executor;
    this.runnerFactory = runnerFactory;
    this.allocator = allocator;
    this.endpointProvider = endpointProvider;
    this.localIdentity = localIdentity;
    this.localAccessAllowed = localAccessAllowed;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public FabricRunnerFactory getRunnerFactory() {
    return runnerFactory;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public Provider<Iterable<NodeEndpoint>> getEndpointProvider() {
    return endpointProvider;
  }

  public NodeEndpoint getLocalIdentity() {
    return localIdentity;
  }

  public boolean isLocalAccessAllowed() {
    return localAccessAllowed;
  }



}
