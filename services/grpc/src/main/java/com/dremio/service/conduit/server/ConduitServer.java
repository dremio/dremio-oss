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
import com.dremio.service.grpc.ContextualizedServerInterceptor;
import com.dremio.ssl.SSLEngineFactory;
import com.dremio.telemetry.utils.TracerFacade;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerServiceDefinition;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import io.netty.handler.ssl.SslContextBuilder;
import io.opentracing.contrib.grpc.TracingServerInterceptor;

/**
 * Conduit server.
 */
public class ConduitServer implements Service {
  private static final Logger logger = LoggerFactory.getLogger(ConduitServer.class);
  private final Provider<ConduitServiceRegistry> registryProvider;
  private final Optional<SSLEngineFactory> sslEngineFactory;

  private final ServerBuilder serverBuilder;
  private final ServerBuilder inProcessServerBuilder;
  private List<CloseableBindableService> closeableServices;

  private volatile Server server = null;
  private volatile Server inProcesServer = null;


  public ConduitServer(
    Provider<ConduitServiceRegistry> registryProvider,
    int port,
    Optional<SSLEngineFactory> sslEngineFactory,
    String inProcessServerName
  ) {
    this.registryProvider = registryProvider;
    this.sslEngineFactory = sslEngineFactory;
    this.serverBuilder = NettyServerBuilder.forPort(port);
    this.closeableServices = new ArrayList<>();
    this.inProcessServerBuilder = InProcessServerBuilder.forName(inProcessServerName);
  }

  public ConduitServer(
    Provider<ConduitServiceRegistry> registryProvider,
    ServerBuilder serverBuilder,
    Optional<SSLEngineFactory> sslEngineFactory,
    String inProcessServerName
  ) {
    this.registryProvider = registryProvider;
    this.sslEngineFactory = sslEngineFactory;
    this.serverBuilder = serverBuilder;
    this.closeableServices = new ArrayList<>();
    this.inProcessServerBuilder = InProcessServerBuilder.forName(inProcessServerName);
  }

  @Override
  public void start() throws Exception {
    final ConduitServiceRegistryImpl registry = (ConduitServiceRegistryImpl) registryProvider.get();

    for (BindableService service : registry.getServiceList()) {
      serverBuilder.addService(service);
      inProcessServerBuilder.addService(service);
    }

    final TracingServerInterceptor tracingServerInterceptor = TracingServerInterceptor.newBuilder().withTracer(TracerFacade.INSTANCE.getTracer()).build();
    serverBuilder.intercept(tracingServerInterceptor);
    serverBuilder.intercept(ContextualizedServerInterceptor.buildBasicContextualizedServerInterceptor());
    inProcessServerBuilder.intercept(tracingServerInterceptor);
    inProcessServerBuilder.intercept(ContextualizedServerInterceptor.buildBasicContextualizedServerInterceptor());

    for (CloseableBindableService closeableService : registry.getCloseableServiceList()) {
      logger.debug("Conduit service being added {}", closeableService.getClass().getName());
      serverBuilder.addService(closeableService);
      inProcessServerBuilder.addService(closeableService);
      closeableServices.add(closeableService);
    }

    for (ServerServiceDefinition serverServiceDefinition : registry.getServerServiceDefinitionList()) {
      serverBuilder.addService(serverServiceDefinition);
      inProcessServerBuilder.addService(serverServiceDefinition);
    }

    registry.getFallbackHandler().ifPresent(fallbackRegistry -> serverBuilder.fallbackHandlerRegistry(fallbackRegistry));

    serverBuilder.maxInboundMetadataSize(Integer.MAX_VALUE).maxInboundMessageSize(Integer.MAX_VALUE)
      .intercept(TransmitStatusRuntimeExceptionInterceptor.instance());

    if (sslEngineFactory.isPresent()) {
      final SslContextBuilder contextBuilder = sslEngineFactory.get().newServerContextBuilder();
      // add gRPC overrides using #configure
      ((NettyServerBuilder)serverBuilder).sslContext(GrpcSslContexts.configure(contextBuilder).build());
    }
    server = serverBuilder.build();
    inProcesServer = inProcessServerBuilder.build();
    server.start();
    inProcesServer.start();

    logger.info("ConduitServer is up. Listening on port '{}'", server.getPort());
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.shutdown();
    }

    if (inProcesServer != null) {
      inProcesServer.shutdown();
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
