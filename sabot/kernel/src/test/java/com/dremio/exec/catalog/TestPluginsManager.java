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
package com.dremio.exec.catalog;

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import javax.inject.Provider;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceInternalData;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.test.DremioTest;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.CheckedFuture;

import io.protostuff.ByteString;

/**
 * Unit tests for PluginsManager.
 */
public class TestPluginsManager {
  private KVStoreProvider storeProvider;
  private PluginsManager plugins;

  @Before
  public void setup() throws Exception {
    storeProvider = new LocalKVStoreProvider(CLASSPATH_SCAN_RESULT, null, true, false);
    storeProvider.start();
    final KVPersistentStoreProvider psp = new KVPersistentStoreProvider(
        new Provider<KVStoreProvider>() {
          @Override
          public KVStoreProvider get() {
            return storeProvider;
          }
        },
        true
    );
    final NamespaceService mockNamespaceService = mock(NamespaceService.class);
    final DatasetListingService mockDatasetListingService = mock(DatasetListingService.class);

    final SabotConfig sabotConfig = SabotConfig.create();
    final SabotContext sabotContext = mock(SabotContext.class);
    // used in c'tor
    when(sabotContext.getClasspathScan())
        .thenReturn(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getNamespaceService(anyString()))
        .thenReturn(mockNamespaceService);
    when(sabotContext.getDatasetListing())
        .thenReturn(mockDatasetListingService);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(SabotConfig.create(), CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence())
        .thenReturn(lpp);
    when(sabotContext.getStoreProvider())
        .thenReturn(psp);

    final SystemOptionManager som = new SystemOptionManager(CLASSPATH_SCAN_RESULT, lpp, psp);
    som.init();
    when(sabotContext.getOptionManager())
        .thenReturn(som);

    // used in start
    when(sabotContext.getKVStoreProvider())
        .thenReturn(storeProvider);
    when(sabotContext.getConfig())
        .thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);

    // used in newPlugin
    when(sabotContext.getRoles())
        .thenReturn(Sets.newHashSet(ClusterCoordinator.Role.MASTER));

    KVStore<NamespaceKey, SourceInternalData> sourceDataStore = storeProvider.getStore(CatalogSourceDataCreator.class);
    plugins = new PluginsManager(sabotContext, sourceDataStore, mock(SchedulerService.class),
      ConnectionReader.of(sabotContext.getClasspathScan(), sabotConfig));
    plugins.start();
  }

  @After
  public void shutdown() throws Exception {
    if (plugins != null) {
      plugins.close();
    }

    if (storeProvider != null) {
      storeProvider.close();
    }
  }

  private static final String INSPECTOR = "inspector";
  private static final ByteString BYTESTRING_UNCHANGED_WITHOUT_DATASET = ByteString.copyFrom(new byte[] {0});
  private static final ByteString BYTESTRING_UNCHANGED_WITH_DATASET = ByteString.copyFrom(new byte[] {1});
  private static final ByteString BYTESTRING_DELETED = ByteString.copyFrom(new byte[] {2});
  private static final ByteString BYTESTRING_CHANGED = ByteString.copyFrom(new byte[] {3});
  private static final DatasetConfig datasetConfig = new DatasetConfig();
  private static final ReadDefinition readDefinition = new ReadDefinition();

  // If this is returned from getTables(), it means that it was the result of calling getTables()
  // on the underlying plugin, not generated by checkReadSignature().
  private static final SourceTableDefinition MOCK_TABLE_DEFINITION = mock(SourceTableDefinition.class);

  @SourceType(value = INSPECTOR, configurable = false)
  public static class Inspector extends ConnectionConf<Inspector, StoragePlugin> {

    @Override
    public StoragePlugin newPlugin(SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      final StoragePlugin mockStoragePlugin = mock(StoragePlugin.class);
      try {
        when(mockStoragePlugin.getDatasets(anyString(), any(DatasetRetrievalOptions.class)))
            .thenReturn(Collections.<SourceTableDefinition>emptyList());

        when(mockStoragePlugin.checkReadSignature(
            eq(BYTESTRING_DELETED),
            eq(datasetConfig),
            any(DatasetRetrievalOptions.class)))
            .thenReturn(StoragePlugin.CheckResult.DELETED);
        when(mockStoragePlugin.checkReadSignature(
            eq(BYTESTRING_UNCHANGED_WITHOUT_DATASET),
            eq(datasetConfig),
            any(DatasetRetrievalOptions.class)))
            .thenReturn(StoragePlugin.CheckResult.UNCHANGED);
        when(mockStoragePlugin.checkReadSignature(
            eq(BYTESTRING_UNCHANGED_WITH_DATASET),
            eq(datasetConfig),
            any(DatasetRetrievalOptions.class)))
            .thenReturn(new StoragePlugin.CheckResult() {
            @Override
            public StoragePlugin.UpdateStatus getStatus() {
              return StoragePlugin.UpdateStatus.UNCHANGED;
            }

            @Override
            public SourceTableDefinition getDataset() {
              return mock(SourceTableDefinition.class);
            }
          });

        when(mockStoragePlugin.checkReadSignature(
            eq(BYTESTRING_CHANGED),
            eq(datasetConfig),
            any(DatasetRetrievalOptions.class)))
            .thenReturn(new StoragePlugin.CheckResult() {
            @Override
            public StoragePlugin.UpdateStatus getStatus() {
              return StoragePlugin.UpdateStatus.UNCHANGED;
            }

            @Override
            public SourceTableDefinition getDataset() {
              return mock(SourceTableDefinition.class);
            }
          });

        when(mockStoragePlugin.getDataset(eq(null), eq(datasetConfig), any(DatasetRetrievalOptions.class)))
            .thenReturn(MOCK_TABLE_DEFINITION);
        when(mockStoragePlugin.getState())
            .thenReturn(SourceState.GOOD);

      } catch (Exception ignored) {
        throw new IllegalStateException("will not throw");
      }
      return mockStoragePlugin;
    }
  }

  @Test
  public void createRefreshDeleteFlow() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey(INSPECTOR);
    final SourceConfig inspectorConfig = new SourceConfig()
        .setType(INSPECTOR)
        .setName(INSPECTOR)
        .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
        .setConfig(new Inspector().toBytesString());

    final KVStore<NamespaceKey, SourceInternalData> kvStore = storeProvider.getStore(CatalogSourceDataCreator.class);
    // must not exist
    assertEquals(null, plugins.get(INSPECTOR));
    assertEquals(null, kvStore.get(sourceKey));

    // create one; lock required
    final ManagedStoragePlugin plugin;
    try (AutoCloseable ignored = plugins.writeLock()) {
      plugin = plugins.create(inspectorConfig);
      plugin.startAsync().checkedGet();
    }
    // "inspector" exists, but it doesn't have any data yet
    assertEquals(0, plugin.getLastFullRefreshDateMs());

    // refresh data
    plugin.initiateMetadataRefresh();
    plugin.refresh(CatalogService.UpdateType.FULL, CatalogService.DEFAULT_METADATA_POLICY);
    final long t1 = System.currentTimeMillis();
    assertTrue(plugin.getLastFullRefreshDateMs() <= t1);

    // next refresh will move the 'last full refresh' timestamp
    plugin.refresh(CatalogService.UpdateType.FULL, CatalogService.DEFAULT_METADATA_POLICY);
    assertTrue(plugin.getLastFullRefreshDateMs() >= t1);

    // delete
    plugins.deleteSource(inspectorConfig);

    // must not exist
    assertEquals(null, plugins.get(INSPECTOR));
  }

  @Test
  public void checkReadSignatureWithDeletedState() throws Exception {
    final NamespaceKey sourceKey = new NamespaceKey(INSPECTOR);
    final SourceConfig inspectorConfig = new SourceConfig()
      .setType(INSPECTOR)
      .setName(INSPECTOR)
      .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
      .setConfig(new Inspector().toBytesString());

    // create one; lock required
    final ManagedStoragePlugin plugin;
    try (AutoCloseable ignored = plugins.writeLock()) {
      plugin = plugins.create(inspectorConfig);
    }
    CheckedFuture<SourceState, Exception> state = plugin.startAsync();
    state.get();

    plugin.initiateMetadataRefresh();
    plugin.refresh(CatalogService.UpdateType.FULL, CatalogService.DEFAULT_METADATA_POLICY);

    readDefinition.setReadSignature(BYTESTRING_DELETED);
    datasetConfig.setReadDefinition(readDefinition);

    assertNull(plugin.getTable(null, datasetConfig, false));

    plugins.deleteSource(inspectorConfig);
  }

  @Test
  public void checkReadSignatureWithUnchangedState() throws Exception {
    final SourceConfig inspectorConfig = new SourceConfig()
      .setType(INSPECTOR)
      .setName(INSPECTOR)
      .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
      .setConfig(new Inspector().toBytesString());

    // create one; lock required
    final ManagedStoragePlugin plugin;
    try (AutoCloseable ignored = plugins.writeLock()) {
      plugin = plugins.create(inspectorConfig);
    }
    CheckedFuture<SourceState, Exception> state = plugin.startAsync();
    state.get();

    plugin.initiateMetadataRefresh();
    plugin.refresh(CatalogService.UpdateType.FULL, CatalogService.DEFAULT_METADATA_POLICY);

    readDefinition.setReadSignature(BYTESTRING_UNCHANGED_WITH_DATASET);
    datasetConfig.setReadDefinition(readDefinition);

    // This should return a non-null SourceTableDefinition that differs from the constant
    // one we have defined before.
    SourceTableDefinition tbl = plugin.getTable(null, datasetConfig, false);
    assertNotNull(tbl);
    assertNotEquals(MOCK_TABLE_DEFINITION, tbl);

    // This signature should return the mock definition.
    readDefinition.setReadSignature(BYTESTRING_UNCHANGED_WITHOUT_DATASET);
    datasetConfig.setReadDefinition(readDefinition);
    tbl = plugin.getTable(null, datasetConfig, false);
    assertEquals(MOCK_TABLE_DEFINITION, tbl);

    plugins.deleteSource(inspectorConfig);
  }

  @Test
  public void checkReadSignatureWithChangedState() throws Exception {
    final SourceConfig inspectorConfig = new SourceConfig()
      .setType(INSPECTOR)
      .setName(INSPECTOR)
      .setMetadataPolicy(CatalogService.DEFAULT_METADATA_POLICY)
      .setConfig(new Inspector().toBytesString());

    // create one; lock required
    final ManagedStoragePlugin plugin;
    try (AutoCloseable ignored = plugins.writeLock()) {
      plugin = plugins.create(inspectorConfig);
    }
    CheckedFuture<SourceState, Exception> state = plugin.startAsync();
    state.get();

    plugin.initiateMetadataRefresh();
    plugin.refresh(CatalogService.UpdateType.FULL, CatalogService.DEFAULT_METADATA_POLICY);

    readDefinition.setReadSignature(BYTESTRING_CHANGED);
    datasetConfig.setReadDefinition(readDefinition);

    // This should return a non-null SourceTableDefinition that differs from the constant
    // one we have defined before.
    SourceTableDefinition tbl = plugin.getTable(null, datasetConfig, false);
    assertNotNull(tbl);
    assertNotEquals(MOCK_TABLE_DEFINITION, tbl);

    plugins.deleteSource(inspectorConfig);
  }
}
