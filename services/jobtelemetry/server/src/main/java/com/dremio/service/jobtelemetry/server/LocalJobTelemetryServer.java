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

package com.dremio.service.jobtelemetry.server;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.jobtelemetry.JobTelemetryRpcUtils;
import com.dremio.service.jobtelemetry.server.store.LocalMetricsStore;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;

import io.grpc.Server;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;

/**
 * Local job telemetry server, running on coordinator node.
 */
public class LocalJobTelemetryServer implements Service {
  private static final Logger logger = LoggerFactory.getLogger(LocalJobTelemetryServer.class);
  private final GrpcServerBuilderFactory grpcFactory;
  private final Provider<LegacyKVStoreProvider> kvStoreProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpoint;
  private ProfileStore profileStore;
  private MetricsStore metricsStore;
  private Server server;

  public LocalJobTelemetryServer(GrpcServerBuilderFactory grpcServerBuilderFactory,
                                 Provider<LegacyKVStoreProvider> kvStoreProvider,
                                 Provider<CoordinationProtos.NodeEndpoint> selfEndpoint) {
    this.grpcFactory = grpcServerBuilderFactory;
    this.kvStoreProvider = kvStoreProvider;
    this.selfEndpoint = selfEndpoint;
  }

  @Override
  public void start() throws Exception {
    profileStore = new LocalProfileStore(kvStoreProvider.get());
    profileStore.start();

    metricsStore = new LocalMetricsStore();
    metricsStore.start();

    server = JobTelemetryRpcUtils.newInProcessServerBuilder(grpcFactory,
      selfEndpoint.get().getFabricPort())
      .maxInboundMetadataSize(81920) // GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE * 10
      .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
      .addService(new JobTelemetryServiceImpl(metricsStore, profileStore, true))
      .build();

    server.start();
    logger.info("LocalJobTelemetryServer is up");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(profileStore, metricsStore,
      () -> {
        if (server != null) {
          server.shutdown();
          server = null;
        }
    });
    logger.info("LocalJobTelemetryServer stopped");
  }
}
