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

package com.dremio.service.conduit.client;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import javax.inject.Provider;
import javax.net.ssl.SSLException;

import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.service.Service;
import com.dremio.service.grpc.DefaultGrpcServiceConfigProvider;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.ssl.SSLEngineFactory;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.util.concurrent.UncheckedExecutionException;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContextBuilder;

/**
 * Master coordinator conduit that provides an active channel to the master coordinator.
 */
public class ConduitProviderImpl implements ConduitProvider, Service {

  private final Provider<NodeEndpoint> masterEndpoint;

  private final LoadingCache<NodeEndpoint, ManagedChannel> managedChannels;

  private final Object lastMasterLock = new Object();
  private volatile NodeEndpoint lastMaster = null;

  // all grpc services that conduit channels should be whitelisted here for
  // retries
  private final List<String> serviceNames = Arrays.asList("dremio.job.JobsService", "dremio" +
    ".catalog.InformationSchemaService", "dremio.job.Chronicle", "dremio.maestroservice.MaestroService");

  public ConduitProviderImpl(
    Provider<NodeEndpoint> masterEndpoint,
    Optional<SSLEngineFactory> sslEngineFactory
  ) {
    this.masterEndpoint = masterEndpoint;
    this.managedChannels = CacheBuilder.newBuilder()
      .removalListener((RemovalListener<NodeEndpoint, ManagedChannel>) notification -> {
        if (!notification.wasEvicted()) {
          notification.getValue().shutdown();
        }
      })
      .build(new CacheLoader<NodeEndpoint, ManagedChannel>() {
        @Override
        public ManagedChannel load(@SuppressWarnings("NullableProblems") NodeEndpoint peerEndpoint)
          throws SSLException {
          final NettyChannelBuilder builder =
            NettyChannelBuilder.forAddress(peerEndpoint.getAddress(), peerEndpoint.getConduitPort())
              .maxInboundMessageSize(Integer.MAX_VALUE)
              .maxInboundMetadataSize(Integer.MAX_VALUE)
              .enableRetry()
              .defaultServiceConfig(DefaultGrpcServiceConfigProvider.getDefaultGrpcServiceConfig(serviceNames))
              .maxRetryAttempts(GrpcChannelBuilderFactory.MAX_RETRY);

          if (sslEngineFactory.isPresent()) {
            final SslContextBuilder contextBuilder = sslEngineFactory.get().newClientContextBuilder();
            // add gRPC overrides using #configure
            builder.useTransportSecurity()
              .sslContext(GrpcSslContexts.configure(contextBuilder).build());
          } else {
            builder.usePlaintext();
          }

          return builder.build();
        }
      });
  }

  @Override
  public ManagedChannel getOrCreateChannel(NodeEndpoint peerEndpoint) {
    Preconditions.checkNotNull(peerEndpoint, "peerEndpoint is required");
    try {
      return managedChannels.get(peerEndpoint);
    } catch (ExecutionException | UncheckedExecutionException e) {
      throw new RuntimeException(String.format("Failed to connect to '%s:%d'",
        peerEndpoint.getAddress(), peerEndpoint.getConduitPort()), e.getCause());
    }
  }

  @Override
  public ManagedChannel getOrCreateChannelToMaster() {
    final NodeEndpoint currentMaster = masterEndpoint.get();
    Preconditions.checkNotNull(currentMaster, "Master coordinator is down");

    NodeEndpoint lastMaster = this.lastMaster;
    if (lastMaster == currentMaster) {
      return getOrCreateChannel(currentMaster);
    }

    // rare operation: master changed, or first init
    synchronized (lastMasterLock) {
      lastMaster = this.lastMaster;

      if (lastMaster == currentMaster) {
        return getOrCreateChannel(currentMaster);
      }

      if (lastMaster != null) { // invalidate stale entry for previous master
        managedChannels.invalidate(lastMaster);
      }

      this.lastMaster = currentMaster;
      return getOrCreateChannel(currentMaster);
    }
  }


  @Override
  public void start() {
  }

  @Override
  public void close() throws Exception {
    managedChannels.invalidateAll(); // shutdowns all connections
  }
}
