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
package com.dremio.service.statistics;

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
import com.dremio.exec.proto.StatisticsRPC;
import com.dremio.exec.store.sys.statistics.StatisticsListManager;
import com.dremio.exec.store.sys.statistics.StatisticsProtocol;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.services.fabric.api.FabricRunnerFactory;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Preconditions;

/**
 * Statistics service
 */
public class StatisticsListManagerImpl implements StatisticsListManager {
  private static final Logger logger = LoggerFactory.getLogger(StatisticsListManagerImpl.class);

  private final Provider<StatisticsService> statisticsServiceProvider;
  private final Provider<Optional<CoordinationProtos.NodeEndpoint>> serviceLeaderProvider;
  private final Provider<FabricService> fabric;
  private final Provider<BufferAllocator> allocatorProvider;
  private final Provider<DremioConfig> dremioConfigProvider;
  private final boolean isMaster;
  private final boolean isCoordinator;
  private StatisticsTunnelCreator statisticsTunnelCreator;

  public StatisticsListManagerImpl(
    Provider<StatisticsService> statisticsServiceProvider,
    Provider<Optional<CoordinationProtos.NodeEndpoint>> serviceLeaderProvider,
    final Provider<FabricService> fabric,
    Provider<BufferAllocator> allocatorProvider,
    Provider<DremioConfig> dremioConfigProvider,
    boolean isMaster,
    boolean isCoordinator
  ) {
    this.statisticsServiceProvider = Preconditions.checkNotNull(statisticsServiceProvider, "statistics service required");
    this.allocatorProvider = Preconditions.checkNotNull(allocatorProvider, "buffer allocator required");
    this.fabric = Preconditions.checkNotNull(fabric, "fabric service required");
    this.serviceLeaderProvider = Preconditions.checkNotNull(serviceLeaderProvider, "serviceLeaderProvider required");
    this.dremioConfigProvider = Preconditions.checkNotNull(dremioConfigProvider, "dremioConfigProvider required");

    this.isMaster = isMaster;
    this.isCoordinator = isCoordinator;
  }

  @Override
  public Iterable<StatisticsListManager.StatisticsInfo> getStatisticsInfos() {
    if (isMaster ||
      (isCoordinator && dremioConfigProvider.get().isMasterlessEnabled())) {
      return statisticsServiceProvider.get().getStatisticsInfos();
    }
    Optional<CoordinationProtos.NodeEndpoint> master = serviceLeaderProvider.get();
    if (!master.isPresent()) {
      throw UserException.connectionError().message("Unable to get task leader while trying to get statistics information")
        .build(logger);
    }
    final StatisticsTunnel statisticsTunnel = statisticsTunnelCreator.getTunnel(master.get());
    try {
      final StatisticsRPC.StatisticsInfoResp statisticsInfoResp =
        statisticsTunnel.requestStatisticsInfos().get(15, TimeUnit.SECONDS);
      return statisticsInfoResp.getStatisticsInfoList().stream()
        .map(StatisticsInfo::fromProto).collect(Collectors.toList());
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      throw UserException.connectionError(e).message("Error while getting statistics information")
        .build(logger);
    }
  }

  @Override
  public void start() throws Exception {
    final FabricRunnerFactory statisticsTunnelFactory = fabric.get().registerProtocol(
      new StatisticsProtocol(allocatorProvider.get(), dremioConfigProvider.get().getSabotConfig(), statisticsServiceProvider.get()));

    statisticsTunnelCreator = new StatisticsTunnelCreator(statisticsTunnelFactory);
  }

  @Override
  public void close() throws Exception {
  }

  /**
   * StatisticsTunnelCreator
   */
  public static class StatisticsTunnelCreator {
    private final FabricRunnerFactory factory;

    public StatisticsTunnelCreator(FabricRunnerFactory factory) {
      super();
      this.factory = factory;
    }

    public StatisticsTunnel getTunnel(CoordinationProtos.NodeEndpoint ep){
      return new StatisticsTunnel(ep, factory.getCommandRunner(ep.getAddress(), ep.getFabricPort()));
    }
  }
}
