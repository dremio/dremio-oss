/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.service.accelerator;

import static com.dremio.service.reflection.ReflectionServiceImpl.LOCAL_TASK_LEADER_NAME;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.BindingCreator;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;

/**
 * Exposes the acceleration manager interface to the rest of the system (executor side)
 */
public class AccelerationListManagerImpl implements AccelerationListManager {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationListManagerImpl.class);

  private final MaterializationStore materializationStore;
  private Provider<SabotContext> contextProvider;
  private Provider<ReflectionStatusService> reflectionStatusService;
  private Provider<ReflectionService> reflectionService;
  private final Provider<FabricService> fabric;
  private final BindingCreator bindingCreator;
  private ReflectionTunnelCreator reflectionTunnelCreator;


  public AccelerationListManagerImpl(Provider<KVStoreProvider> storeProvider, Provider<SabotContext> contextProvider,
                                     Provider<ReflectionStatusService> reflectionStatusService,
                                     Provider<ReflectionService> reflectionService,
                                     final Provider<FabricService> fabric,
                                     final BindingCreator bindingCreator
  ) {
    this.materializationStore = new MaterializationStore(storeProvider);
    this.contextProvider = contextProvider;
    this.reflectionStatusService = reflectionStatusService;
    this.reflectionService = reflectionService;
    this.fabric = fabric;
    this.bindingCreator = bindingCreator;
  }

  @Override
  public void start() {
    final FabricRunnerFactory reflectionTunnelFactory = fabric.get().registerProtocol(new ReflectionProtocol
      (contextProvider.get().getAllocator(), reflectionStatusService.get(), reflectionService.get(),
        materializationStore, contextProvider.get().getConfig()));

    reflectionTunnelCreator = new ReflectionTunnelCreator(reflectionTunnelFactory);
    bindingCreator.bindSelf(reflectionTunnelCreator);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public Iterable<ReflectionInfo> getReflections() {
    if (contextProvider.get().isMaster() ||
      (contextProvider.get().isCoordinator() &&
        contextProvider.get().getDremioConfig().isMasterlessEnabled())) {
      return reflectionStatusService.get().getReflections();
    }
    // need to do RPC call
    // trying to get master
    Optional<CoordinationProtos.NodeEndpoint> master = contextProvider.get().getServiceLeader(LOCAL_TASK_LEADER_NAME);
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get Reflection Information")
        .build(logger);
    }
    final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master.get());
    try {
      final ReflectionRPC.ReflectionInfoResp reflectionCombinedStatusResp =
        reflectionTunnel.requestReflectionStatus().get(15, TimeUnit.SECONDS);
      return reflectionCombinedStatusResp.getReflectionInfoList().stream()
        .map(ReflectionInfo::getReflectionInfo).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting Reflection Information")
        .build(logger);
    }
   }

  @Override
  public Iterable<DependencyInfo> getReflectionDependencies() {
    if (contextProvider.get().isMaster() ||
      (contextProvider.get().isCoordinator() &&
        contextProvider.get().getDremioConfig().isMasterlessEnabled())) {
      return reflectionService.get().getReflectionDependencies();
    }
    // need to do RPC call
    // trying to get master
    Optional<CoordinationProtos.NodeEndpoint> master = contextProvider.get().getServiceLeader(LOCAL_TASK_LEADER_NAME);
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get Reflection Information")
        .build(logger);
    }
    final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master.get());
    try {
      final ReflectionRPC.DependencyInfoResp dependencyInfosResp =
        reflectionTunnel.requestDependencyInfos().get(15, TimeUnit.SECONDS);
      return dependencyInfosResp.getDependencyInfoList().stream()
        .map(DependencyInfo::getDependencyInfo).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting Dependency Information")
        .build(logger);
    }
   }

  @Override
  public Iterable<MaterializationInfo> getMaterializations() {
    if (contextProvider.get().isMaster() ||
      (contextProvider.get().isCoordinator() &&
        contextProvider.get().getDremioConfig().isMasterlessEnabled())) {
      return AccelerationMaterializationUtils.getMaterializationsFromStore(materializationStore);
    }
    // need to do RPC call
    // trying to get task leader
    Optional<CoordinationProtos.NodeEndpoint> taskLeader = contextProvider.get().getServiceLeader(LOCAL_TASK_LEADER_NAME);
    if (!taskLeader.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get Reflection Information")
        .build(logger);
    }
    final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(taskLeader.get());
    try {
      final ReflectionRPC.MaterializationInfoResp materializationInfosResp =
        reflectionTunnel.requestMaterializationInfos().get(15, TimeUnit.SECONDS);
      return materializationInfosResp.getMaterializationInfoList().stream()
        .map(MaterializationInfo::fromMaterializationInfo).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting Materialization Information")
        .build(logger);
    }
  }

  @Override
  public Iterable<RefreshInfo> getRefreshInfos() {
    if (contextProvider.get().isMaster() ||
      (contextProvider.get().isCoordinator() &&
        contextProvider.get().getDremioConfig().isMasterlessEnabled())) {
      return StreamSupport.stream(reflectionStatusService.get().getRefreshInfos().spliterator(), false)
        .map(RefreshInfo::fromRefreshInfo).collect(Collectors.toList());
    }
    // need to do RPC call
    // trying to get master
    Optional<CoordinationProtos.NodeEndpoint> master = contextProvider.get().getServiceLeader(LOCAL_TASK_LEADER_NAME);
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get Reflection Information")
        .build(logger);
    }
    final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master.get());
    try {
      final ReflectionRPC.RefreshInfoResp refreshInfosResp =
        reflectionTunnel.requestRefreshInfos().get(15, TimeUnit.SECONDS);
      return refreshInfosResp.getRefreshInfoList().stream()
        .map(RefreshInfo::fromRefreshInfo).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting Refresh Information")
        .build(logger);
    }
  }
}
