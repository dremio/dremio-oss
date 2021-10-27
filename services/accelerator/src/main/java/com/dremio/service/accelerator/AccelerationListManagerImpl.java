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
package com.dremio.service.accelerator;

import java.util.Iterator;
import java.util.Optional;

import javax.inject.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.acceleration.ReflectionDescriptionServiceGrpc;
import com.dremio.service.acceleration.ReflectionDescriptionServiceRPC;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.collect.Streams;

/**
 * Exposes the acceleration manager interface to the rest of the system (executor side)
 */
public class AccelerationListManagerImpl implements AccelerationListManager {
  private static final Logger logger = LoggerFactory.getLogger(AccelerationListManagerImpl.class);

  private final MaterializationStore materializationStore;
  private Provider<ReflectionStatusService> reflectionStatusService;
  private final Provider<ReflectionService> reflectionService;

  private final Provider<DremioConfig> dremioConfigProvider;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private final Provider<Optional<NodeEndpoint>> serviceLeaderProvider;
  private final Provider<ConduitProvider> conduitProvider;

  public AccelerationListManagerImpl(
      Provider<LegacyKVStoreProvider> storeProvider,
      Provider<ReflectionStatusService> reflectionStatusService,
      Provider<ReflectionService>  reflectionService,
      Provider<DremioConfig> dremioConfigProvider,
      boolean isMaster,
      boolean isCoordinator,
      Provider<Optional<NodeEndpoint>> serviceLeaderProvider,
      Provider<ConduitProvider> conduitProvider
  ) {
    this.materializationStore = new MaterializationStore(storeProvider);
    this.reflectionStatusService = reflectionStatusService;
    this.reflectionService = reflectionService;
    this.dremioConfigProvider = dremioConfigProvider;
    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
    this.serviceLeaderProvider = serviceLeaderProvider;
    this.conduitProvider = conduitProvider;
  }


  @Override
  public void start() {
  }

  public ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceBlockingStub getAccelerationListServiceBlockingStub() {
    ReflectionDescriptionServiceGrpc.ReflectionDescriptionServiceBlockingStub accelerationListServiceBlockingStub;

    Optional<CoordinationProtos.NodeEndpoint> master = serviceLeaderProvider.get();
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get Reflection Information")
        .build(logger);
    }
    accelerationListServiceBlockingStub = ReflectionDescriptionServiceGrpc.newBlockingStub(this.conduitProvider.get().getOrCreateChannel(master.get()));

    return accelerationListServiceBlockingStub;
  }


  @Override
  public void close() throws Exception {

  }

  @Override
  public Iterator<ReflectionInfo> getReflections() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return reflectionStatusService.get().getReflections();
    }

    // need to do RPC call
    try {
      ReflectionDescriptionServiceRPC.ListReflectionsRequest reflectionInfoReq = ReflectionDescriptionServiceRPC.ListReflectionsRequest.newBuilder().build();
      final Iterator<ReflectionDescriptionServiceRPC.ListReflectionsResponse> reflectionCombinedStatusResp = getAccelerationListServiceBlockingStub().listReflections(reflectionInfoReq);
      return Streams.stream(reflectionCombinedStatusResp).map(ReflectionInfo::getReflectionInfo).iterator();
    } catch (Exception e) {
      throw UserException.connectionError(e).message("Error while getting Reflection Information")
        .build(logger);
    }
   }

  @Override
  public Iterator<DependencyInfo> getReflectionDependencies() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return reflectionService.get().getReflectionDependencies();
    }

    // need to do RPC call
    try {
      ReflectionDescriptionServiceRPC.ListReflectionDependenciesRequest dependencyInfoReq = ReflectionDescriptionServiceRPC.ListReflectionDependenciesRequest.newBuilder().build();
      final Iterator<ReflectionDescriptionServiceRPC.ListReflectionDependenciesResponse> dependencyInfosResp = getAccelerationListServiceBlockingStub().listReflectionDependencies(dependencyInfoReq);
      return Streams.stream(dependencyInfosResp).map(DependencyInfo::getDependencyInfo).iterator();
    } catch (Exception e) {
      throw UserException.connectionError(e).message("Error while getting Dependency Information")
        .build(logger);
    }
   }

  @Override
  public Iterator<MaterializationInfo> getMaterializations() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return AccelerationMaterializationUtils.getMaterializationsFromStore(materializationStore);
    }

    // need to do RPC call
    try {
      ReflectionDescriptionServiceRPC.ListMaterializationsRequest materializationInfoReq = ReflectionDescriptionServiceRPC.ListMaterializationsRequest.newBuilder().build();
      final Iterator<ReflectionDescriptionServiceRPC.ListMaterializationsResponse> materializationInfosResp = getAccelerationListServiceBlockingStub().listMaterializations(materializationInfoReq);
      return Streams.stream(materializationInfosResp).map(MaterializationInfo::fromMaterializationInfo).iterator();
    } catch (Exception e) {
      throw UserException.connectionError(e).message("Error while getting Materialization Information")
        .build(logger);
    }
  }

  @Override
  public Iterator<RefreshInfo> getRefreshInfos() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return Streams.stream(reflectionStatusService.get().getRefreshInfos())
        .map(RefreshInfo::fromRefreshInfo).iterator();
    }

    // need to do RPC call
    try {
      ReflectionDescriptionServiceRPC.GetRefreshInfoRequest refreshInfoReq = ReflectionDescriptionServiceRPC.GetRefreshInfoRequest.newBuilder().build();
      final Iterator<ReflectionDescriptionServiceRPC.GetRefreshInfoResponse> refreshInfosResp = getAccelerationListServiceBlockingStub().getRefreshInfo(refreshInfoReq);
      return Streams.stream(refreshInfosResp).map(RefreshInfo::fromRefreshInfo).iterator();
    } catch (Exception e) {
      throw UserException.connectionError(e).message("Error while getting Refresh Information")
        .build(logger);
    }
  }
}
