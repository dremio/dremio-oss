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
package com.dremio.exec.server;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.exec.expr.ExpressionSplitCache;
import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.QueryContextCreator;
import com.dremio.exec.planner.RulesFactory;
import com.dremio.exec.planner.cost.RelMetadataQuerySupplier;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.store.sys.statistics.StatisticsAdministrationService;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.exec.work.WorkStats;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.resource.GroupResourceInformation;
import com.dremio.resource.common.ReflectionRoutingManager;
import com.dremio.service.coordinator.CoordinatorModeInfo;
import com.dremio.service.namespace.NamespaceService;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;

/**
 * An interface used by {@link SabotContext} that will be used by {@link
 * com.dremio.exec.ops.QueryContext}. It is only to be used inside the Planner. The goal is to start
 * to remove items from {@link SabotContext} that can be handled through dependency injection.
 */
public interface SabotQueryContext {
  OptionValidatorListing getOptionValidatorListing();

  SystemOptionManager getSystemOptionManager();

  CoordinationProtos.NodeEndpoint getEndpoint();

  SabotConfig getConfig();

  FunctionImplementationRegistry getDecimalFunctionImplementationRegistry();

  FunctionImplementationRegistry getFunctionImplementationRegistry();

  BufferAllocator getQueryPlanningAllocator();

  CatalogService getCatalogService();

  Provider<RelMetadataQuerySupplier> getRelMetadataQuerySupplier();

  AccelerationManager getAccelerationManager();

  ReflectionRoutingManager getReflectionRoutingManager();

  StatisticsService getStatisticsService();

  Provider<StatisticsAdministrationService.Factory> getStatisticsAdministrationFactoryProvider();

  Collection<RulesFactory> getInjectedRulesFactories();

  LogicalPlanPersistence getLpPersistence();

  Collection<CoordinationProtos.NodeEndpoint> getExecutors();

  DremioConfig getDremioConfig();

  Provider<SimpleJobRunner> getJobsRunner();

  OptionManager getOptionManager();

  boolean isUserAuthenticationEnabled();

  ScanResult getClasspathScan();

  Provider<MaterializationDescriptorProvider> getMaterializationProvider();

  Provider<WorkStats> getWorkStatsProvider();

  ExpressionSplitCache getExpressionSplitCache();

  ExecutorService getExecutorService();

  NamespaceService getNamespaceService(String userName);

  Provider<CoordinatorModeInfo> getCoordinatorModeInfoProvider();

  GroupResourceInformation getClusterResourceInformation();

  QueryContextCreator getQueryContextCreator();
}
