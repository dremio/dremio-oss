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

import java.io.IOException;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService.ContextMigratingCloseableExecutorService;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.Service;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.annotations.VisibleForTesting;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;

/**
 * A simple service to register PDFS Remote FS fabric protocol
 */
public class PDFSService implements Service {
  private final Provider<FabricService> fabricService;
  private final SabotConfig config;
  private final BufferAllocator allocator;
  private final Provider<NodeEndpoint> identityProvider;
  private final Provider<Iterable<NodeEndpoint>> nodeProvider;
  private final boolean allowLocalAccess;
  private final Tracer tracer;
  private CloseableExecutorService pool;

  public PDFSService(
      Provider<FabricService> fabricService,
      Provider<NodeEndpoint> identityProvider,
      Provider<Iterable<NodeEndpoint>> nodeProvider,
      Tracer tracer,
      SabotConfig config,
      BufferAllocator allocator,
      PDFSMode mode) {
    this.fabricService = fabricService;
    this.identityProvider = identityProvider;
    this.nodeProvider = nodeProvider;
    this.tracer = tracer;
    this.config = config;
    this.allocator = allocator.newChildAllocator("pdfs-allocator", 0, Long.MAX_VALUE);
    this.allowLocalAccess = mode == PDFSMode.DATA;
  }

  @VisibleForTesting
  public PDFSService(
      Provider<FabricService> fabricService,
      Provider<NodeEndpoint> nodeEndpointProvider,
      Provider<Iterable<NodeEndpoint>> executorsProvider,
      SabotConfig config,
      BufferAllocator allocator
  ) {
    this(fabricService, nodeEndpointProvider, executorsProvider, NoopTracerFactory.create(), config, allocator, PDFSMode.DATA);
  }

  @Override
  public void start() throws Exception {
    FabricService fabricService = this.fabricService.get();

    pool = new ContextMigratingCloseableExecutorService<>(new CloseableThreadPool("pdfs"));

    FabricRunnerFactory factory = fabricService.registerProtocol(PDFSProtocol.newInstance(identityProvider.get(), this.config, allocator, allowLocalAccess));

    final PDFSConfig config = new PDFSConfig(pool, factory, allocator, nodeProvider, identityProvider.get(), allowLocalAccess);
    PseudoDistributedFileSystem.configure(config);

    // A hack until DX-4639 is addressed.
    createFileSystem();
  }

  public FileSystem createFileSystem() throws IOException{
    Configuration c = new Configuration();
    FileSystem.setDefaultUri(c, "pdfs:///");
    return FileSystem.get(c);
  }

  @Override
  public void close() throws Exception {
    // If pool is null, service is not started yet
    if (pool == null) {
      allocator.close();
      return;
    }

    // DX-5178
    // we have to synchronize here so the pool is either open or closed since elsewhere a static reference is taken.
    synchronized(pool){
      AutoCloseables.close(pool, allocator);
    }
  }

  /**
   * Enumeration of possible PDFS modes.
   */
  public static enum PDFSMode {
    /**
     * A mode where services acts solely as a client to connect to remove nodes.
     */
    CLIENT,

    /**
     * A mode where this node stores data locally as part of a cluster.
     */
    DATA
  }

}
