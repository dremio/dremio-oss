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

package com.dremio.service.conduit.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.service.Service;
import com.dremio.service.grpc.CloseableBindableService;
import com.dremio.ssl.SSLEngineFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Conduit server.
 */
public class ConduitServer implements Service {
  private static final Logger logger = LoggerFactory.getLogger(ConduitServer.class);

  private final Provider<ConduitServiceRegistry> registryProvider;
  private final Optional<SSLEngineFactory> sslEngineFactory;

  private final NettyServerBuilder serverBuilder;
  private List<CloseableBindableService> closeableServices;

  private volatile Server server = null;

  public ConduitServer(
    Provider<ConduitServiceRegistry> registryProvider,
    int port,
    Optional<SSLEngineFactory> sslEngineFactory
  ) {
    this.registryProvider = registryProvider;
    this.sslEngineFactory = sslEngineFactory;
    this.serverBuilder = NettyServerBuilder.forPort(port);
    this.closeableServices = new ArrayList<>();
  }

  @Override
  public void start() throws Exception {
    final ConduitServiceRegistryImpl registry = (ConduitServiceRegistryImpl) registryProvider.get();

    for (BindableService service : registry.getServiceList()) {
      serverBuilder.addService(service);
    }

    for (CloseableBindableService closeableService : registry.getCloseableServiceList()) {
      logger.debug("Conduit service being added {}", closeableService.getClass().getName());
      serverBuilder.addService(closeableService);
      closeableServices.add(closeableService);
    }

    serverBuilder.maxInboundMetadataSize(Integer.MAX_VALUE).maxInboundMessageSize(Integer.MAX_VALUE)
      .intercept(TransmitStatusRuntimeExceptionInterceptor.instance());

    if (sslEngineFactory.isPresent()) {
      final SslContextBuilder contextBuilder = sslEngineFactory.get().newServerContextBuilder();
      // add gRPC overrides using #configure
      serverBuilder.sslContext(GrpcSslContexts.configure(contextBuilder).build());
    }
    server = serverBuilder.build();
    server.start();

    logger.info("ConduitServer is up. Listening on port '{}'", server.getPort());
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.shutdown();
    }

    AutoCloseables.close(closeableServices);
  }

  public int getPort() {
    Preconditions.checkNotNull(server, "ConduitServer was not started (see #start)");
    return server.getPort();
  }

  @VisibleForTesting
  Server getServer() {
    return server;
  }

  public Optional<SSLEngineFactory> getSslEngineFactory() {
    return this.sslEngineFactory;
  }
}
