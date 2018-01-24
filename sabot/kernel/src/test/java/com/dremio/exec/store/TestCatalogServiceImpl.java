/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.ExecTest;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.BindingCreator;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.scheduler.SchedulerService;

/**
 * Unit tests for CatalogServiceImpl
 */
public class TestCatalogServiceImpl {
  private KVStoreProvider kvStore;
  private CatalogServiceImpl catalogServiceImpl;

  @Before
  public void setup() throws Exception {
    kvStore = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);;
    kvStore.start();
    final KVPersistentStoreProvider pStoreProvider = new KVPersistentStoreProvider(
      new Provider<KVStoreProvider>() {
        @Override
        public KVStoreProvider get() {
          return kvStore;
        }
      }
      , true
    );

    final NamespaceService mockNamespaceService = mock(NamespaceService.class);

    final SabotContext sabotContext = mock(SabotContext.class);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(mockNamespaceService);
    when(sabotContext.getClasspathScan()).thenReturn(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence()).thenReturn(new LogicalPlanPersistence(SabotConfig.create(), CLASSPATH_SCAN_RESULT));
    when(sabotContext.getStoreProvider()).thenReturn(pStoreProvider);
    when(sabotContext.getConfig()).thenReturn(ExecTest.DEFAULT_SABOT_CONFIG);

    final SystemTablePluginConfigProvider sysPlugin = new SystemTablePluginConfigProvider();
    final Provider<SystemTablePluginConfigProvider> sysPluginProvider = new Provider<SystemTablePluginConfigProvider>() {
      @Override
      public SystemTablePluginConfigProvider get() {
        return sysPlugin;
      }
    };
    SchedulerService mockSchedulerService = mock(SchedulerService.class);
    catalogServiceImpl = new CatalogServiceImpl(DirectProvider.wrap(sabotContext), DirectProvider.wrap(mockSchedulerService),
      DirectProvider.wrap(kvStore), mock(BindingCreator.class), false, true, sysPluginProvider);
    catalogServiceImpl.start();
  }

  @After
  public void shutdown() throws Exception {
    if (kvStore != null) {
      kvStore.close();
    }
  }

  @Test
  public void testCatalogKvStore() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey("test");
    StoragePlugin mockStoragePlugin = mock(StoragePlugin.class);
    when(mockStoragePlugin.getDatasets(anyString(), anyBoolean())).thenReturn(Collections.<SourceTableDefinition>emptyList());

    assertEquals(null, catalogServiceImpl.getSourceLastFullRefreshDate(sourceKey));
    catalogServiceImpl.registerSource(sourceKey, mockStoragePlugin);
    // Entry for 'sourceKey' exists, but it doesn't have any data yet
    assertEquals(null, catalogServiceImpl.getSourceLastFullRefreshDate(sourceKey));

    catalogServiceImpl.refreshSource(sourceKey, CatalogService.DEFAULT_METADATA_POLICY);
    long t1 = System.currentTimeMillis();
    assert(catalogServiceImpl.getSourceLastFullRefreshDate(sourceKey) <= t1);

    // Next refresh will move the 'last full refresh' timestamp
    catalogServiceImpl.refreshSource(sourceKey, CatalogService.DEFAULT_METADATA_POLICY);
    assert(catalogServiceImpl.getSourceLastFullRefreshDate(sourceKey) >= t1);

    catalogServiceImpl.unregisterSource(sourceKey);
    assertEquals(null, catalogServiceImpl.getSourceLastFullRefreshDate(sourceKey));
  }
}
