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
package com.dremio.dac.metadata;

import static java.lang.Math.abs;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.model.sources.SourceUI;
import com.dremio.dac.model.sources.UIMetadataPolicy;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.CatalogServiceImpl.UpdateType;
import com.dremio.exec.catalog.ManagedStoragePlugin;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.NASConf;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.google.common.base.Stopwatch;


/**
 * Test last metadata refresh date is synced on multiple coordinators
 **/
public class TestRefreshForMultiCoordinators extends BaseTestServer {
  private static final String PLUGIN_NAME = "testRefresh";
  private MetadataPolicy metadataPolicy;
  private SourceConfig sourceConfig;
  private static CatalogServiceImpl masterCatalogService;
  private static CatalogServiceImpl slaveCatalogService;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @BeforeClass
  public static void init() throws Exception {
    // ignore the tests if not multinode.
    assumeTrue(isMultinode());
    BaseTestServer.init();

    masterCatalogService = ((CatalogServiceImpl) getMasterDremioDaemon().getBindingProvider().lookup(CatalogService.class));
    slaveCatalogService = ((CatalogServiceImpl) getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class));
  }

  /**
   * Setup source config for create a source
   **/
  private void setUpForCreateSource() throws Exception {
    metadataPolicy = new MetadataPolicy()
      .setAuthTtlMs(MINUTES.toMillis(10))
      .setDeleteUnavailableDatasets(true)
      .setAutoPromoteDatasets(true)
      .setDatasetUpdateMode(UpdateMode.PREFETCH_QUERIED)
      .setNamesRefreshMs(HOURS.toMillis(2))
      .setDatasetDefinitionRefreshAfterMs(HOURS.toMillis(3))
      .setDatasetDefinitionExpireAfterMs(DAYS.toMillis(14));

    final SourceUI source = new SourceUI();
    source.setName(PLUGIN_NAME);
    source.setCtime(1000L);
    source.setMetadataPolicy(UIMetadataPolicy.of(metadataPolicy));

    temporaryFolder.create();
    final NASConf config = new NASConf();
    config.path = temporaryFolder.getRoot().getAbsolutePath();
    source.setConfig(config);
    sourceConfig  = source.asSourceConfig();
  }

  /**
   * Add a source on master and perform a full refresh on master.
   * (Note: adding a source will perform a name refresh)
   * Checking the last refresh info on slave coordinator is synced
   * up with master coordinator.
   *
   * Delete the source and add a source with the same name on slave
   * coordinator, do the same check above on master coordinator.
  **/
  @Test
  public void testLastRefreshSyncedOnMultiCoordinators() throws Exception {
    // master add a source and perform a full refresh
    setUpForCreateSource();
    masterCatalogService.createSourceIfMissingWithThrow(sourceConfig);
    masterCatalogService.refreshSource(new NamespaceKey(PLUGIN_NAME), metadataPolicy, UpdateType.FULL);
    Thread.sleep(3_000);

    // retrieve the added plugin from slave coordinator's catalog service
    // and compare with the one from master
    ManagedStoragePlugin masterPlugin = masterCatalogService.getManagedSource(PLUGIN_NAME);
    ManagedStoragePlugin slavePlugin = masterCatalogService.getManagedSource(PLUGIN_NAME);

    Stopwatch watch = Stopwatch.createStarted();
    while (masterPlugin == null || slavePlugin == null) {
      if (watch.elapsed(MINUTES) > 1) {
        throw new Exception("Failed to retrieve plugin");
      }
      Thread.sleep(2_000);
      masterPlugin = masterCatalogService.getManagedSource(PLUGIN_NAME);
      slavePlugin = slaveCatalogService.getManagedSource(PLUGIN_NAME);
    }
    watch.stop();
    // if the plugin's last refresh date in master and slave is within 1 second apart
    // then consider coordinators are synced
    //assertTrue(slavePlugin.isCompleteAndValid());
    long masterLastFullRefresh = masterPlugin.getLastFullRefreshDateMs();
    long masterLastNameRefresh = masterPlugin.getLastNamesRefreshDateMs();
    long slaveLastFullRefresh = slavePlugin.getLastFullRefreshDateMs();
    long slaveLastNameRefresh = slavePlugin.getLastNamesRefreshDateMs();
    assertTrue(String.format("Last full refresh date for plugin '%s' on slave coordinator is not synced: master Coordinator's" +
        " last full refresh is at %d while slave coordinator's is at %d.", PLUGIN_NAME, masterLastFullRefresh, slaveLastFullRefresh),
      abs(masterLastFullRefresh - slaveLastFullRefresh) < 1000);
    assertTrue(String.format("Last name refresh date for plugin '%s' on slave coordinator is not synced: master Coordinator's" +
        " last name refresh is at %d while slave coordinator's is at %d.", PLUGIN_NAME, masterLastNameRefresh, slaveLastNameRefresh),
      abs(masterLastNameRefresh - slaveLastNameRefresh) < 1000);


    // delete the source
    masterCatalogService.deleteSource(PLUGIN_NAME);
    Thread.sleep(3_000);

    // slave add a source with the same name and perform a full refresh
    setUpForCreateSource();
    slaveCatalogService.createSourceIfMissingWithThrow(sourceConfig);
    slaveCatalogService.refreshSource(new NamespaceKey(PLUGIN_NAME), metadataPolicy, UpdateType.FULL);
    Thread.sleep(3_000);

    // retrieve the added plugin from master coordinator's catalog service
    // and compare with the one from slave
    masterPlugin = masterCatalogService.getManagedSource(PLUGIN_NAME);
    slavePlugin = slaveCatalogService.getManagedSource(PLUGIN_NAME);

    watch.start();
    while (masterPlugin == null || slavePlugin == null) {
      if (watch.elapsed(MINUTES) > 1) {
        throw new Exception("Failed to retrieve plugin");
      }
      Thread.sleep(2_000);
      masterPlugin = masterCatalogService.getManagedSource(PLUGIN_NAME);
      slavePlugin = slaveCatalogService.getManagedSource(PLUGIN_NAME);
    }
    watch.stop();
    // if the plugin's last refresh date in slave and master is within 1 second apart
    // then consider coordinators are synced
    masterLastFullRefresh = masterPlugin.getLastFullRefreshDateMs();
    masterLastNameRefresh = masterPlugin.getLastNamesRefreshDateMs();
    slaveLastFullRefresh = slavePlugin.getLastFullRefreshDateMs();
    slaveLastNameRefresh = slavePlugin.getLastNamesRefreshDateMs();
    assertTrue(String.format("Last full refresh date for plugin '%s' on master coordinator is not synced: master Coordinator's" +
        " last full refresh is at %d while slave coordinator's is at %d.", PLUGIN_NAME, masterLastFullRefresh, slaveLastFullRefresh),
      abs(masterLastFullRefresh - slaveLastFullRefresh) < 1000);
    assertTrue(String.format("Last name refresh date for plugin '%s' on master coordinator is not synced: master Coordinator's" +
        " last name refresh is at %d while slave coordinator's is at %d.", PLUGIN_NAME, masterLastNameRefresh, slaveLastNameRefresh),
      abs(masterLastNameRefresh - slaveLastNameRefresh) < 1000);
  }
}
