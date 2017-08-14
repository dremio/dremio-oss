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
import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.calcite.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.SqlUtils;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.BindingCreator;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.TableInstance;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Unit tests for {@link StoragePluginRegistryImpl}. Currently it only tests
 * {@link StoragePluginRegistryImpl#refreshSourceMetadataInNamespace(String)}. We can add more
 * tests as we fix issues or add features.
 */
@Ignore("TODO (AH)")
public class TestStoragePluginRegistryImpl {

  private KVStoreProvider kvStore;
  private StoragePluginRegistryImpl registry;
  private NamespaceService ns;
  private StoragePlugin testStoragePlugin;
  private StoragePlugin2 testStoragePlugin2;
  private NamespaceKey testPluginKey;
  private CatalogService catalogService;
  private SchedulerService schedulerService;

  private final List<DatasetConfig> mockDatasets =
      ImmutableList.of(
          newDataset("test.fld1.ds11"),
          newDataset("test.fld1.ds12"),
          newDataset("test.fld2.fld21.ds211"),
          newDataset("test.fld2.ds22"),
          newDataset("test.ds3")
      );

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

    final SabotContext sabotContext = mock(SabotContext.class);

    ns = new NamespaceServiceImpl(kvStore);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(ns);
    when(sabotContext.getClasspathScan()).thenReturn(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence()).thenReturn(new LogicalPlanPersistence(SabotConfig.create(), CLASSPATH_SCAN_RESULT));
    schedulerService = new LocalSchedulerService();
    catalogService = new CatalogServiceImpl(DirectProvider.wrap(sabotContext), DirectProvider.wrap(schedulerService), mock(BindingCreator.class), false, true);

    registry = new StoragePluginRegistryImpl(sabotContext, catalogService, pStoreProvider);

    testStoragePlugin2 = mock(StoragePlugin2.class);
    testStoragePlugin = mock(StoragePlugin.class);
    when(testStoragePlugin.getStoragePlugin2()).thenReturn(testStoragePlugin2);

    testPluginKey = new NamespaceKey("test");
    SourceConfig testSourceConfig = new SourceConfig()
      .setName("test")
      .setCtime(100L)
      .setType(SourceType.NAS);

    ns.addOrUpdateSource(testPluginKey, testSourceConfig);
    catalogService.registerSource(testPluginKey, testStoragePlugin.getStoragePlugin2());
  }

  @After
  public void shutdown() throws Exception {
    if (kvStore != null) {
      kvStore.close();
    }
  }

  @Test
  public void refreshSourceMetadata_EmptySource() throws Exception {
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has no datasets under "test"
    List<NamespaceKey> datasets = ns.getAllDatasets(testPluginKey);
    assertEquals(0, datasets.size());

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime() throws Exception {
    // Mock test storage plugin to return few datasets
    when(testStoragePlugin.listDatasets()).thenReturn(mockDatasets);

    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(mockDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21"));

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_UpdateWithNewDatasets() throws Exception {
    // Mock test storage plugin to return few datasets
    when(testStoragePlugin.listDatasets()).thenReturn(mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    List<DatasetConfig> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld1.ds13"));
    testDatasets.add(newDataset("test.fld2.fld21.ds212"));
    testDatasets.add(newDataset("test.fld5.ds51"));

    when(testStoragePlugin.listDatasets()).thenReturn(testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(9, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21", "test.fld5"));

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_MultipleUpdatesWithNewDatasetsDeletedDatasets() throws Exception {
    when(testStoragePlugin.listDatasets()).thenReturn(mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    List<DatasetConfig> testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.fld22.ds222"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld5.ds51"));

    when(testStoragePlugin.listDatasets()).thenReturn(testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld22", "test.fld5"));
    assertFoldersDonotExist(asList("test.fld2.fld21"));

    // Make another refresh
    testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.fld2.ds23"));
    testDatasets.add(newDataset("test.ds5"));
    testDatasets.add(newDataset("test.fld5.ds51"));
    testDatasets.add(newDataset("test.fld5.ds52"));
    testDatasets.add(newDataset("test.fld6.ds61"));

    when(testStoragePlugin.listDatasets()).thenReturn(testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(7, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld5", "test.fld6"));
    assertFoldersDonotExist(asList("test.fld2.fld22"));

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_MultipleUpdatesWithImplicitDatasets() throws Exception {
    when(testStoragePlugin.listDatasets()).thenReturn(mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // add couple of implicit datasets to Namespace under test source
    final List<DatasetConfig> implicitDatasets = asList(
        newDataset("test.fld2.fld21.ds212"),
        newDataset("test.fld3.ds31"),
        newDataset("test.ds5")
    );
    ns.tryCreatePhysicalDataset(new NamespaceKey(asList("test", "fld2", "fld21", "ds212")), implicitDatasets.get(0));
    ns.tryCreatePhysicalDataset(new NamespaceKey(asList("test", "fld3", "ds31")), implicitDatasets.get(1));
    ns.tryCreatePhysicalDataset(new NamespaceKey(asList("test", "ds5")), implicitDatasets.get(2));

    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(8, actualDatasetKeys.size());

    assertDatasetsAreEqual(copyOf(Iterables.concat(mockDatasets, implicitDatasets)), actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21", "test.fld3"));

    List<DatasetConfig> testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.fld22.ds222"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld5.ds51"));

    when(testStoragePlugin.listDatasets()).thenReturn(testDatasets);
    when(testStoragePlugin.getDataset(anyList(), any(TableInstance.class), any(SchemaConfig.class))).thenAnswer(
        new Answer<DatasetConfig>() {
          @Override
          public DatasetConfig answer(InvocationOnMock invocationOnMock) throws Throwable {
            List<String> path = invocationOnMock.getArgumentAt(0, List.class);
            for(DatasetConfig ds : implicitDatasets) {
              if (ds.getFullPathList().equals(path)) {
                return ds;
              }
            }
            return null;
          }
        }
    );
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(8, actualDatasetKeys.size());

    assertDatasetsAreEqual(copyOf(Iterables.concat(testDatasets, implicitDatasets)), actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21", "test.fld2.fld22", "test.fld3", "test.fld5"));

    // Make another refresh
    testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.fld2.ds23"));
    testDatasets.add(newDataset("test.ds5"));
    testDatasets.add(newDataset("test.fld5.ds51"));
    testDatasets.add(newDataset("test.fld5.ds52"));
    testDatasets.add(newDataset("test.fld6.ds61"));

    when(testStoragePlugin.listDatasets()).thenReturn(testDatasets);
    when(testStoragePlugin.getDataset(anyList(), any(TableInstance.class), any(SchemaConfig.class))).thenAnswer(
        new Answer<DatasetConfig>() {
          @Override
          public DatasetConfig answer(InvocationOnMock invocationOnMock) throws Throwable {
            List<String> path = invocationOnMock.getArgumentAt(0, List.class);
            if (path.equals(asList("test", "fld3", "ds31"))) {
              return newDataset("test.fld3.ds31");
            }
            return null;
          }
        }
    );

    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(8, actualDatasetKeys.size());

    assertDatasetsAreEqual(copyOf(Iterables.concat(testDatasets, implicitDatasets.subList(1, 2))), actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld3", "test.fld5", "test.fld6"));
    assertFoldersDonotExist(asList("test.fld2.fld22", "test.fld2.fld21"));

    assertNoDatasetsAfterSourceDeletion();
  }

  private static DatasetConfig newDataset(final String dsPath) {
    final List<String> path = SqlUtils.parseSchemaPath(dsPath);
    return new DatasetConfig()
        .setName(Util.last(path))
        .setFullPathList(path)
        .setType(DatasetType.PHYSICAL_DATASET)
        .setRecordSchema(ByteString.EMPTY)
        .setPhysicalDataset(
            new PhysicalDataset()
            .setFormatSettings(null))
        .setSchemaVersion(DatasetHelper.CURRENT_VERSION)
       ;

  }

  private static void assertDatasetsAreEqual(List<DatasetConfig> expDatasets, List<NamespaceKey> actualDatasetKeys) {
    final Set<NamespaceKey> expDatasetKeys = FluentIterable.from(expDatasets)
        .transform(new Function<DatasetConfig, NamespaceKey>() {
          @Nullable
          @Override
          public NamespaceKey apply(@Nullable DatasetConfig input) {
            return new NamespaceKey(input.getFullPathList());
          }
        }).toSet();
    assertEquals(expDatasetKeys, Sets.newHashSet(actualDatasetKeys));
  }

  private void assertFoldersExist(List<String> expFolders) throws Exception {
    for(String folderPath : expFolders) {
      NamespaceKey folderKey = new NamespaceKey(SqlUtils.parseSchemaPath(folderPath));
      ns.getFolder(folderKey); // if the folder doesn't exit we get an exception
    }
  }

  private void assertFoldersDonotExist(List<String> expFolders) throws Exception {
    for(String folderPath : expFolders) {
      NamespaceKey folderKey = new NamespaceKey(SqlUtils.parseSchemaPath(folderPath));
      try {
        ns.getFolder(folderKey); // if the folder doesn't exit we get an exception
        fail();
      } catch (NamespaceNotFoundException ex) { /* no-op */ }
    }
  }

  private void assertNoDatasetsAfterSourceDeletion() throws Exception {
    final SourceConfig sourceConfig = ns.getSource(testPluginKey);
    ns.deleteSource(testPluginKey, sourceConfig.getVersion());

    assertEquals(0, ns.getAllDatasets(testPluginKey).size());
  }
}
