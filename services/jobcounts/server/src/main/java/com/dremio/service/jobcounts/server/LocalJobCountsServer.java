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

package com.dremio.service.jobcounts.server;

import com.dremio.common.AutoCloseables;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.service.Service;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.jobcounts.JobCountsRpcUtils;
import com.dremio.service.jobcounts.server.store.JobCountStore;
import com.dremio.service.jobcounts.server.store.JobCountStoreImpl;
import io.grpc.Server;
import io.grpc.util.TransmitStatusRuntimeExceptionInterceptor;
import javax.inject.Provider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Local job counts server, running on coordinator node. */
public class LocalJobCountsServer implements Service {
  private static final Logger logger = LoggerFactory.getLogger(LocalJobCountsServer.class);
  private final GrpcServerBuilderFactory grpcFactory;
  private final Provider<KVStoreProvider> kvStoreProvider;
  private final Provider<CoordinationProtos.NodeEndpoint> selfEndpoint;
  private JobCountStore jobCountStore;
  private Server server;

  public LocalJobCountsServer(
      GrpcServerBuilderFactory grpcServerBuilderFactory,
      Provider<KVStoreProvider> kvStoreProvider,
      Provider<CoordinationProtos.NodeEndpoint> selfEndpoint) {
    this.grpcFactory = grpcServerBuilderFactory;
    this.kvStoreProvider = kvStoreProvider;
    this.selfEndpoint = selfEndpoint;
  }

  @Override
  public void start() throws Exception {
    jobCountStore = new JobCountStoreImpl(kvStoreProvider);
    jobCountStore.start();

    server =
        JobCountsRpcUtils.newInProcessServerBuilder(grpcFactory, selfEndpoint.get().getFabricPort())
            .maxInboundMetadataSize(81920) // GrpcUtil.DEFAULT_MAX_HEADER_LIST_SIZE * 10
            .intercept(TransmitStatusRuntimeExceptionInterceptor.instance())
            .addService(new JobCountsServiceImpl(jobCountStore))
            .build();

    server.start();
    logger.info("LocalJobCountsServer is up");
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(
        jobCountStore,
        () -> {
          if (server != null) {
            server.shutdown();
            server = null;
          }
        });
    logger.info("LocalJobCountsServer stopped");
  }

  public static class NoopLocalJobCountsServer extends LocalJobCountsServer {

    public NoopLocalJobCountsServer() {
      super(null, null, null);
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}
  }
  ;
}
