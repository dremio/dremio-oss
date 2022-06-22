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
package com.dremio.exec.store.sys.udf;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.FunctionRPC;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Preconditions;

/**
 * UserDefinedFunctionListManagerImpl
 */
public class UserDefinedFunctionListManagerImpl implements UserDefinedFunctionListManager {
  private static final Logger logger = LoggerFactory.getLogger(UserDefinedFunctionListManagerImpl.class);
  private final Provider<NamespaceService> namespaceServiceProvider;
  private final Provider<Optional<CoordinationProtos.NodeEndpoint>> serviceLeaderProvider;
  private final Provider<FabricService> fabric;
  private final Provider<BufferAllocator> allocatorProvider;
  private final Provider<DremioConfig> dremioConfigProvider;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private FunctionTunnelCreator functionTunnelCreator;

  public UserDefinedFunctionListManagerImpl(
    Provider<NamespaceService> namespaceServiceProvider,
    Provider<Optional<CoordinationProtos.NodeEndpoint>> serviceLeaderProvider,
    final Provider<FabricService> fabric,
    Provider<BufferAllocator> allocatorProvider,
    Provider<DremioConfig> dremioConfigProvider,
    boolean isMaster,
    boolean isCoordinator
  ) {
    this.namespaceServiceProvider = Preconditions.checkNotNull(namespaceServiceProvider, "NamespaceService service required");
    this.allocatorProvider = Preconditions.checkNotNull(allocatorProvider, "buffer allocator required");
    this.fabric = Preconditions.checkNotNull(fabric, "fabric service required");
    this.serviceLeaderProvider = Preconditions.checkNotNull(serviceLeaderProvider, "serviceLeaderProvider required");
    this.dremioConfigProvider = Preconditions.checkNotNull(dremioConfigProvider, "dremioConfigProvider required");
    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
  }

  @Override
  public Iterable<FunctionInfo> getFunctionInfos() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return namespaceServiceProvider.get().getFunctions().stream().map(UserDefinedFunctionListManager::getFunctionInfoFromConfig).
        collect(Collectors.toList());
    }
    Optional<CoordinationProtos.NodeEndpoint> master = serviceLeaderProvider.get();
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get function information")
        .build(logger);
    }
    final FunctionTunnel udfTunnel = functionTunnelCreator.getTunnel(master.get());
    try {
      final FunctionRPC.FunctionInfoResp udfInfoResp =
        udfTunnel.requestFunctionInfos().get(15, TimeUnit.SECONDS);
      return udfInfoResp.getFunctionInfoList().stream()
        .map(UserDefinedFunctionListManager.FunctionInfo::fromProto).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting function information")
        .build(logger);
    }
  }
  @Override
  public void start() throws Exception {
    final FabricRunnerFactory udfTunnelFactory = fabric.get().registerProtocol(
      new FunctionProtocol(allocatorProvider.get(), dremioConfigProvider.get().getSabotConfig(), namespaceServiceProvider));
    functionTunnelCreator = new FunctionTunnelCreator(udfTunnelFactory);
  }
  @Override
  public void close() throws Exception {
  }
  /**
   * UdfTunnelCreator
   */
  public static class FunctionTunnelCreator {
    private final FabricRunnerFactory factory;
    public FunctionTunnelCreator(FabricRunnerFactory factory) {
      super();
      this.factory = factory;
    }
    public FunctionTunnel getTunnel(CoordinationProtos.NodeEndpoint ep){
      return new FunctionTunnel(ep, factory.getCommandRunner(ep.getAddress(), ep.getFabricPort()));
    }
  }


}
