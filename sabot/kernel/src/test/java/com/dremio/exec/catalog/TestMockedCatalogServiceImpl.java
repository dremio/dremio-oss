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

package com.dremio.exec.catalog;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.EnumSet;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Test;

import com.dremio.common.exceptions.UserException;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.server.SabotContext;
import com.dremio.options.OptionManager;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.fabric.api.FabricService;

public class TestMockedCatalogServiceImpl {

  @Test
  public void testGetStorageRulesWithPluginInBadState() throws Exception {
    CatalogServiceImpl catalogService = spy(new CatalogServiceImpl(
      () -> mock(SabotContext.class),
      () -> mock(SchedulerService.class),
      () -> () -> mock(ConnectionConf.class),
      () -> () -> mock(ConnectionConf.class),
      () -> mock(FabricService.class),
      () -> mock(ConnectionReader.class),
      () -> mock(BufferAllocator.class),
      () -> mock(LegacyKVStoreProvider.class),
      () -> mock(DatasetListingService.class),
      () -> mock(OptionManager.class),
      () -> mock(MetadataRefreshInfoBroadcaster.class),
      mock(DremioConfig.class),
      mock(EnumSet.class),
      mock(CatalogServiceMonitor.class),
      mock(Provider.class),
      () -> mock(VersionedDatasetAdapterFactory.class)));
    QueryContext queryContext = mock(QueryContext.class);
    when(queryContext.getCatalogService()).thenReturn(catalogService);

    OptimizerRulesContext optimizerRulesContext = mock(OptimizerRulesContext.class);
    PlannerPhase plannerPhase = mock(PlannerPhase.class);

    ManagedStoragePlugin plugin = mock(ManagedStoragePlugin.class);
    when(plugin.getId()).thenThrow(UserException.sourceInBadState()
      .message("Plugin in bad state was not ignored.")
      .buildSilently());

    when(plugin.getState()).thenReturn(SourceState.GOOD);

    PluginsManager plugins = mock(PluginsManager.class);
    when(plugins.managed()).thenReturn(Arrays.asList(plugin));

    when(catalogService.getPlugins()).thenReturn(plugins);

    catalogService.getStorageRules(optimizerRulesContext, plannerPhase);
  }
}
