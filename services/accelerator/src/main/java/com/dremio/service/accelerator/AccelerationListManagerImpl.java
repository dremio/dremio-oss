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

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;
import javax.inject.Provider;

import com.dremio.common.exceptions.UserException;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.ProtostuffSerializer;
import com.dremio.datastore.Serializer;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ReflectionRPC;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.accel.AccelerationListManager;
import com.dremio.service.BindingCreator;
import com.dremio.service.job.proto.JoinAnalysis;
import com.dremio.service.reflection.ReflectionService;
import com.dremio.service.reflection.ReflectionStatusService;
import com.dremio.service.reflection.ReflectionUtils;
import com.dremio.service.reflection.proto.DataPartition;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.store.MaterializationStore;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;

/**
 * Exposes the acceleration manager interface to the rest of the system (executor side)
 */
public class AccelerationListManagerImpl implements AccelerationListManager {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationListManagerImpl.class);

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
      (contextProvider.get().getAllocator(), reflectionStatusService.get(), reflectionService.get(), contextProvider.get().getConfig()));

    reflectionTunnelCreator = new ReflectionTunnelCreator(reflectionTunnelFactory);
    bindingCreator.bindSelf(reflectionTunnelCreator);
  }

  @Override
  public void close() throws Exception {

  }

  @Override
  public Iterable<ReflectionInfo> getReflections() {
    if(!contextProvider.get().isCoordinator()) {
      // need to do RPC call
      // trying to get master
      CoordinationProtos.NodeEndpoint master = null;
      for (CoordinationProtos.NodeEndpoint coordinator : contextProvider.get().getCoordinators()) {
        if (coordinator.getRoles().getMaster()) {
          master = coordinator;
          break;
        }
      }
      if (master == null) {
        throw UserException.connectionError().message("Unable to get master while trying to get Reflection Information")
          .build(logger);
      }
      final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master);
      try {
        final ReflectionRPC.ReflectionInfoResp reflectionCombinedStatusResp =
          reflectionTunnel.requestReflectionStatus().get(15, TimeUnit.SECONDS);
        FluentIterable<ReflectionInfo> reflections = FluentIterable.from(reflectionCombinedStatusResp
          .getReflectionInfoList()).transform(new Function<ReflectionRPC.ReflectionInfo, ReflectionInfo>() {
          @Nullable
          @Override
          public ReflectionInfo apply(@Nullable ReflectionRPC.ReflectionInfo reflectionInfo) {
            return ReflectionInfo.getReflectionInfo(reflectionInfo);
          }
        });
        return reflections;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw UserException.connectionError(e).message("Error while getting Reflection Information")
          .build(logger);
      }
      // in this case at least we will
    } else {
      return reflectionStatusService.get().getReflections();
    }
  }

  @Override
  public Iterable<DependencyInfo> getReflectionDependencies() {
    if(!contextProvider.get().isMaster()) {
      // need to do RPC call
      // trying to get master
      CoordinationProtos.NodeEndpoint master = null;
      for (CoordinationProtos.NodeEndpoint coordinator : contextProvider.get().getCoordinators()) {
        if (coordinator.getRoles().getMaster()) {
          master = coordinator;
          break;
        }
      }
      if (master == null) {
        throw UserException.connectionError().message("Unable to get master while trying to get Dependency Information")
          .build(logger);
      }
      final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master);
      try {
        final ReflectionRPC.DependencyInfoResp dependencyInfosResp =
          reflectionTunnel.requestDependencyInfos().get(15, TimeUnit.SECONDS);
        FluentIterable<DependencyInfo> dependencyInfos = FluentIterable.from(dependencyInfosResp
          .getDependencyInfoList()).transform(new Function<ReflectionRPC.DependencyInfo, DependencyInfo>() {
          @Override
          public DependencyInfo apply(ReflectionRPC.DependencyInfo dependencyInfo) {
            return DependencyInfo.getDependencyInfo(dependencyInfo);
          }
        });
        return dependencyInfos;
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        throw UserException.connectionError(e).message("Error while getting Dependency Information")
          .build(logger);
      }
      // in this case at least we will
    } else {
      return reflectionService.get().getReflectionDependencies();
    }
  }

  private static final Serializer<JoinAnalysis> JOIN_ANALYSIS_SERIALIZER = ProtostuffSerializer.of(JoinAnalysis.getSchema());

  @Override
  public Iterable<MaterializationInfo> getMaterializations() {
    return FluentIterable.from(ReflectionUtils.getAllMaterializations(materializationStore))
      .transform(new Function<Materialization, MaterializationInfo>() {
        @Override
        public MaterializationInfo apply(Materialization materialization) {
          long footPrint = -1L;
          try {
            footPrint = materializationStore.getMetrics(materialization).getFootprint();
          } catch (Exception e) {
            // let's not fail the query if we can't retrieve the footprint for one materialization
          }

          String joinAnalysisJson = null;
          try {
            if (materialization.getJoinAnalysis() != null) {
              joinAnalysisJson = JOIN_ANALYSIS_SERIALIZER.toJson(materialization.getJoinAnalysis());
            }
          } catch (IOException e) {
            logger.debug("Failed to serialize join analysis", e);
          }

          final String failureMsg = materialization.getFailure() != null ? materialization.getFailure().getMessage() : null;

          return new MaterializationInfo(
            materialization.getReflectionId().getId(),
            materialization.getId().getId(),
            new Timestamp(materialization.getCreatedAt()),
            new Timestamp(Optional.fromNullable(materialization.getExpiration()).or(0L)),
            footPrint,
            materialization.getSeriesId(),
            materialization.getInitRefreshJobId(),
            materialization.getSeriesOrdinal(),
            joinAnalysisJson,
            materialization.getState().toString(),
            Optional.fromNullable(failureMsg).or("NONE"),
            dataPartitionsToString(materialization.getPartitionList()),
            new Timestamp(Optional.fromNullable(materialization.getLastRefreshFromPds()).or(0L))
          );
        }
      });
  }

  private String dataPartitionsToString(List<DataPartition> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      return "";
    }

    final StringBuilder dataPartitions = new StringBuilder();
    for (int i = 0; i < partitions.size() - 1; i++) {
      dataPartitions.append(partitions.get(i).getAddress()).append(", ");
    }
    dataPartitions.append(partitions.get(partitions.size() - 1).getAddress());
    return dataPartitions.toString();
  }

  @Override
  public Iterable<RefreshInfo> getRefreshInfos() {
    if (contextProvider.get().isCoordinator()) {
      return FluentIterable.from(reflectionStatusService.get().getRefreshInfos()).transform(new Function<ReflectionRPC.RefreshInfo, RefreshInfo>() {
        @Nullable
        @Override
        public RefreshInfo apply(@Nullable ReflectionRPC.RefreshInfo refreshInfo) {
          return RefreshInfo.fromRefreshInfo(refreshInfo);
        }
      });
    }
    // need to do RPC call
    // trying to get master
    CoordinationProtos.NodeEndpoint master = null;
    for (CoordinationProtos.NodeEndpoint coordinator : contextProvider.get().getCoordinators()) {
      if (coordinator.getRoles().getMaster()) {
        master = coordinator;
        break;
      }
    }
    if (master == null) {
      throw UserException.connectionError().message("Unable to get master while trying to get Reflection Information")
        .build(logger);
    }
    final ReflectionTunnel reflectionTunnel = reflectionTunnelCreator.getTunnel(master);
    try {
      final ReflectionRPC.RefreshInfoResp refreshInfosResp =
        reflectionTunnel.requestRefreshInfos().get(15, TimeUnit.SECONDS);
      FluentIterable<RefreshInfo> refreshInfos = FluentIterable.from(refreshInfosResp.getRefreshInfoList())
        .transform(new Function<ReflectionRPC.RefreshInfo, RefreshInfo>() {
        @Nullable
        @Override
        public RefreshInfo apply(@Nullable ReflectionRPC.RefreshInfo refreshInfo) {
          return RefreshInfo.fromRefreshInfo(refreshInfo);
        }
      });
      return refreshInfos;
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting Refresh Information")
        .build(logger);
    }
  }
}
