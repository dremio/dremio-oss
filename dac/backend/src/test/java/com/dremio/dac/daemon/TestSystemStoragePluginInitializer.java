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
package com.dremio.dac.daemon;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.TestCatalogServiceImpl;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.InternalFileConf;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * To test SystemStoragePlugins init
 */
public class TestSystemStoragePluginInitializer {

  private static final String HOSTNAME = "localhost";
  private static final int THREAD_COUNT = 2;
  private static final long RESERVATION = 0;
  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static final int TIMEOUT = 0;

  private static TestCatalogServiceImpl.MockUpPlugin mockUpPlugin;

  private ConnectionReader reader;

  private KVStoreProvider storeProvider;
  private NamespaceService namespaceService;
  private BufferAllocator allocator;
  private LocalClusterCoordinator clusterCoordinator;
  private CloseableThreadPool pool;
  private FabricService fabricService;
  private CatalogService catalogService;


  @Before
  public void setup() throws Exception {
    final SabotConfig sabotConfig = SabotConfig.create();
    final SabotContext sabotContext = mock(SabotContext.class);

    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    storeProvider.start();
    final KVPersistentStoreProvider psp = new KVPersistentStoreProvider(DirectProvider.wrap(storeProvider), true);
    when(sabotContext.getStoreProvider())
      .thenReturn(psp);

    namespaceService = new NamespaceServiceImpl(storeProvider);
    when(sabotContext.getNamespaceService(anyString()))
      .thenReturn(namespaceService);

    when(sabotContext.getClasspathScan())
      .thenReturn(CLASSPATH_SCAN_RESULT);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence())
      .thenReturn(lpp);

    final SystemOptionManager som = new SystemOptionManager(CLASSPATH_SCAN_RESULT, lpp, psp);
    som.init();
    when(sabotContext.getOptionManager())
      .thenReturn(som);

    when(sabotContext.getKVStoreProvider())
      .thenReturn(storeProvider);
    when(sabotContext.getConfig())
      .thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);

    allocator = RootAllocatorFactory.newRoot(sabotConfig);
    when(sabotContext.getAllocator())
      .thenReturn(allocator);

    clusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
    when(sabotContext.getClusterCoordinator())
      .thenReturn(clusterCoordinator);
    when(sabotContext.getExecutors())
      .thenReturn(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.EXECUTOR)
        .getAvailableEndpoints());
    when(sabotContext.getCoordinators())
      .thenReturn(clusterCoordinator.getServiceSet(ClusterCoordinator.Role.COORDINATOR)
        .getAvailableEndpoints());

    when(sabotContext.getRoles())
      .thenReturn(Sets.newHashSet(ClusterCoordinator.Role.MASTER, ClusterCoordinator.Role.COORDINATOR));

    pool = new CloseableThreadPool("catalog-test");
    fabricService = new FabricServiceImpl(HOSTNAME, 45678, true, THREAD_COUNT, allocator, RESERVATION,
        MAX_ALLOCATION, TIMEOUT, pool);

    reader = ConnectionReader.of(DremioTest.CLASSPATH_SCAN_RESULT, DremioTest.DEFAULT_SABOT_CONFIG);

    catalogService = new CatalogServiceImpl(
      DirectProvider.wrap(sabotContext),
      DirectProvider.wrap((SchedulerService) new LocalSchedulerService(1)),
      DirectProvider.wrap(new SystemTablePluginConfigProvider()),
      DirectProvider.wrap(fabricService),
      DirectProvider.wrap(reader));
    catalogService.start();
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(catalogService, fabricService, pool, clusterCoordinator,
      allocator, storeProvider);
  }

  @Test
  public void refreshSystemPluginsTest() throws Exception {

    SystemStoragePluginInitializer systemInitializer = new SystemStoragePluginInitializer();

    SourceConfig c = new SourceConfig();
    InternalFileConf conf = new InternalFileConf();
    conf.connection = "classpath:///";
    conf.path = "/";
    conf.isInternal = false;
    conf.propertyList = ImmutableList.of(new Property("abc", "bcd"), new Property("def", "123"));
    c.setName("mytest");
    c.setConnectionConf(conf);
    c.setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);

    systemInitializer.createOrUpdateSystemSource(catalogService, namespaceService, c);

    final CatalogServiceImpl catalog = (CatalogServiceImpl) catalogService;

    SourceConfig updatedC = new SourceConfig();
    InternalFileConf updatedCConf = new InternalFileConf();
    updatedCConf.connection = "file:///";
    updatedCConf.path = "/";
    updatedCConf.isInternal = true;
    updatedC.setName("mytest");
    updatedC.setConnectionConf(updatedCConf);
    updatedC.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);

    final SourceConfig config = catalog.getManagedSource("mytest").getId().getClonedConfig();
    InternalFileConf decryptedConf = (InternalFileConf) reader.getConnectionConf(config);

    systemInitializer.createOrUpdateSystemSource(catalogService, namespaceService, updatedC);

    final SourceConfig updatedConfig = catalog.getManagedSource("mytest").getId().getClonedConfig();
    InternalFileConf decryptedUpdatedConfig = (InternalFileConf) reader.getConnectionConf(updatedConfig);

    assertNotNull(decryptedConf.getProperties());
    assertEquals(2, decryptedConf.getProperties().size());
    assertTrue(decryptedUpdatedConfig.getProperties().isEmpty());
    assertNotEquals(config.getMetadataPolicy(), updatedConfig.getMetadataPolicy());
    assertNotEquals(decryptedConf.getConnection(), decryptedUpdatedConfig.getConnection());
    assertEquals("file:///", decryptedUpdatedConfig.getConnection());
    assertNotEquals(decryptedConf.isInternal, decryptedUpdatedConfig.isInternal);
    assertEquals(decryptedConf.path, decryptedUpdatedConfig.path);
    assertEquals(config.getVersion().longValue()+1, updatedConfig.getVersion().longValue());

    SourceConfig updatedC2 = new SourceConfig();
    InternalFileConf updatedCConf2 = new InternalFileConf();
    updatedCConf2.connection = "file:///";
    updatedCConf2.path = "/";
    updatedCConf2.isInternal = true;
    updatedC2.setName("mytest");
    updatedC2.setConnectionConf(updatedCConf2);
    updatedC2.setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY);

    systemInitializer.createOrUpdateSystemSource(catalogService, namespaceService, updatedC2);

    final SourceConfig updatedConfig2 = catalog.getManagedSource("mytest").getId().getClonedConfig();

    InternalFileConf decryptedConf2 = (InternalFileConf) reader.getConnectionConf(updatedConfig2);
    assertTrue(decryptedConf2.getProperties().isEmpty());

    assertEquals(updatedConfig.getVersion().longValue(), updatedConfig2.getVersion().longValue());


    catalog.deleteSource("mytest");
    assertNull(catalog.getManagedSource("myTest"));
  }

}
