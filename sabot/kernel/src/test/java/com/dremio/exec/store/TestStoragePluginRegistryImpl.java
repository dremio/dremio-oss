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
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.utils.SqlUtils;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.sys.SystemTablePluginProvider;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.BindingCreator;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.SourceType;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Unit tests for {@link StoragePluginRegistryImpl}. Currently it only tests
 * {@link StoragePluginRegistryImpl#refreshSourceMetadataInNamespace(String)}. We can add more
 * tests as we fix issues or add features.
 */
public class TestStoragePluginRegistryImpl {

  private KVStoreProvider kvStore;
  private StoragePluginRegistryImpl registry;
  private NamespaceService ns;
  private StoragePlugin2 testStoragePlugin;
  private NamespaceKey testPluginKey;
  private CatalogService catalogService;
  private SchedulerService schedulerService;

  private final List<SourceTableDefinition> mockDatasets =
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
    when(sabotContext.getStoreProvider()).thenReturn(pStoreProvider);

    final SystemTablePluginProvider sysPlugin = new SystemTablePluginProvider(new Provider<SabotContext>() {
      @Override
      public SabotContext get() {
        return sabotContext;
      }
    });
    final Provider<SystemTablePluginProvider> sysPluginProvider = new Provider<SystemTablePluginProvider>() {
      @Override
      public SystemTablePluginProvider get() {
        return sysPlugin;
      }
    };
    final StoragePluginRegistryImpl[] bindResult = new StoragePluginRegistryImpl[1];
    BindingCreator mockBindingCreator = mock(BindingCreator.class);
    when(mockBindingCreator.bind(isA(Class.class), any())).then(new Answer<StoragePluginRegistry>() {
      public StoragePluginRegistry answer(InvocationOnMock invocation) {
        StoragePluginRegistryImpl registry = invocation.getArgumentAt(1, StoragePluginRegistryImpl.class);
        bindResult[0] = registry;
        return registry;
      }
    });

    schedulerService = new LocalSchedulerService(1);
    catalogService = new CatalogServiceImpl(DirectProvider.wrap(sabotContext), DirectProvider.wrap(schedulerService),
      DirectProvider.wrap(kvStore), mockBindingCreator, false, true, sysPluginProvider);

    catalogService.start();
    registry = bindResult[0];

    testStoragePlugin = new TestStoragePlugin2();

    testPluginKey = new NamespaceKey("test");
    SourceConfig testSourceConfig = new SourceConfig()
      .setName("test")
      .setCtime(100L)
      .setType(SourceType.NAS);

    ns.addOrUpdateSource(testPluginKey, testSourceConfig);
    catalogService.registerSource(testPluginKey, testStoragePlugin);
  }

  @After
  public void shutdown() throws Exception {
    if (kvStore != null) {
      kvStore.close();
    }
  }

  @Test
  public void refreshSourceMetadata_EmptySource() throws Exception {
    final List<SourceTableDefinition> emptyDatasets = ImmutableList.of();
    doMockDatasets(testStoragePlugin, emptyDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has no datasets under "test"
    List<NamespaceKey> datasets = ns.getAllDatasets(testPluginKey);
    assertEquals(0, datasets.size());

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime() throws Exception {
    doMockDatasets(testStoragePlugin, mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(mockDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_UpdateWithNewDatasets() throws Exception {
    doMockDatasets(testStoragePlugin, mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(5, actualDatasetKeys.size());

    List<SourceTableDefinition> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld1.ds13"));
    testDatasets.add(newDataset("test.fld2.fld21.ds212"));
    testDatasets.add(newDataset("test.fld5.ds51"));

    doMockDatasets(testStoragePlugin, testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(9, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21", "test.fld5"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_MultipleUpdatesWithNewDatasetsDeletedDatasets() throws Exception {
    doMockDatasets(testStoragePlugin, mockDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    List<SourceTableDefinition> testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.fld22.ds222"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld5.ds51"));

    doMockDatasets(testStoragePlugin, testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld22", "test.fld5"));
    assertFoldersDonotExist(asList("test.fld2.fld21"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    // Make another refresh
    testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset("test.fld1.ds11"));
    testDatasets.add(newDataset("test.fld2.ds22"));
    testDatasets.add(newDataset("test.fld2.ds23"));
    testDatasets.add(newDataset("test.ds5"));
    testDatasets.add(newDataset("test.fld5.ds51"));
    testDatasets.add(newDataset("test.fld5.ds52"));
    testDatasets.add(newDataset("test.fld6.ds61"));

    doMockDatasets(testStoragePlugin, testDatasets);
    registry.refreshSourceMetadataInNamespace("test", CatalogService.REFRESH_EVERYTHING_NOW);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(7, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld5", "test.fld6"));
    assertFoldersDonotExist(asList("test.fld2.fld22"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

   // Test whether name refresh will get new source dataset names, without refreshing the metadata for the datasets
  @Test
  public void refreshSourceNames() throws Exception {
    NamespaceKey testSourceKey = new NamespaceKey("test");
    doMockDatasets(testStoragePlugin, mockDatasets);
    catalogService.refreshSourceNames(testSourceKey, CatalogService.DEFAULT_METADATA_POLICY);

    assertEquals(5, ns.getAllDatasets(testPluginKey).size());

    List<SourceTableDefinition> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset("test.fld1.ds13"));
    testDatasets.add(newDataset("test.fld2.fld21.ds212"));
    testDatasets.add(newDataset("test.fld2.ds23"));
    testDatasets.add(newDataset("test.ds4"));
    testDatasets.add(newDataset("test.fld5.ds51"));
    doMockDatasets(testStoragePlugin, testDatasets);
    catalogService.refreshSourceNames(testSourceKey, CatalogService.DEFAULT_METADATA_POLICY);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = ns.getAllDatasets(testPluginKey);
    assertEquals(10, actualDatasetKeys.size());
    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(asList("test.fld1", "test.fld2", "test.fld2.fld21", "test.fld5"));
    assertDatasetSchemasNotDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  private static SourceTableDefinition newDataset(final String dsPath) {
    final List<String> path = SqlUtils.parseSchemaPath(dsPath);

    SourceTableDefinition ret = mock(SourceTableDefinition.class);
    NamespaceKey datasetName = new NamespaceKey(path);
    when(ret.getName()).thenReturn(datasetName);

    BatchSchema schema = BatchSchema.newBuilder()
      .addField(new Field("string", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))
      .build();
    DatasetConfig dsConfig = new DatasetConfig()
      .setName(Util.last(path))
      .setFullPathList(path)
      .setType(DatasetType.PHYSICAL_DATASET_SOURCE_FILE)
      .setRecordSchema(ByteString.EMPTY)
      .setPhysicalDataset(
        new PhysicalDataset()
          .setFormatSettings(null))
      .setSchemaVersion(DatasetHelper.CURRENT_VERSION)
      .setRecordSchema(schema.toByteString())
      .setReadDefinition(new ReadDefinition());
    try {
      when(ret.getDataset()).thenReturn(dsConfig);
    } catch (Exception ex) {}

    when(ret.getType()).thenReturn(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);

    when(ret.isSaveable()).thenReturn(true);
    return ret;
  }

  private void doMockDatasets(StoragePlugin2 plugin, final List<SourceTableDefinition> datasets) throws Exception {
    ((TestStoragePlugin2) plugin).setDatasets(datasets);
  }

  private static void assertDatasetsAreEqual(List<SourceTableDefinition> expDatasets, List<NamespaceKey> actualDatasetKeys) {
    final Set<NamespaceKey> expDatasetKeys = FluentIterable.from(expDatasets)
      .transform(new Function<SourceTableDefinition, NamespaceKey>() {
        @Nullable
        @Override
        public NamespaceKey apply(@Nullable SourceTableDefinition input) {
          return input.getName();
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

  private void assertDatasetSchemasDefined(List<NamespaceKey> datasetKeys) throws Exception {
    for (NamespaceKey datasetKey : datasetKeys) {
      DatasetConfig dsConfig = ns.getDataset(datasetKey);
      BatchSchema schema = BatchSchema.deserialize(dsConfig.getRecordSchema());
      assertEquals(schema.getFieldCount(), 1);
      assertEquals(schema.getColumn(0).getName(), "string");
      assertEquals(schema.getColumn(0).getType(), ArrowType.Utf8.INSTANCE);
    }
  }

  private void assertDatasetSchemasNotDefined(List<NamespaceKey> datasetKeys) throws Exception {
    for (NamespaceKey datasetKey : datasetKeys) {
      DatasetConfig dsConfig = ns.getDataset(datasetKey);
      assertEquals(dsConfig.getRecordSchema(), null);
    }
  }

  private void assertNoDatasetsAfterSourceDeletion() throws Exception {
    final SourceConfig sourceConfig = ns.getSource(testPluginKey);
    ns.deleteSource(testPluginKey, sourceConfig.getVersion());

    assertEquals(0, ns.getAllDatasets(testPluginKey).size());
  }

  class TestStoragePlugin2 implements StoragePlugin2 {
    private List<SourceTableDefinition> datasets = null;

    public TestStoragePlugin2() {}
    public void setDatasets(List<SourceTableDefinition> datasets) {
      this.datasets = datasets;
    }

    public Iterable<SourceTableDefinition> getDatasets(String user, boolean ignoreAuthErrors) throws Exception {
      return datasets;
    }
    public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset, boolean ignoreAuthErrors) throws Exception {
      throw new UnsupportedOperationException("NYI");
    }
    public boolean containerExists(NamespaceKey key) {
      throw new UnsupportedOperationException("NYI");
    }
    public boolean datasetExists(NamespaceKey key) {
      for (SourceTableDefinition definition : datasets) {
        if (definition.getName().equals(key)) {
          return true;
        }
      }
      return false;
    }
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      throw new UnsupportedOperationException("NYI");
    }
    public SourceState getState() {
      throw new UnsupportedOperationException("NYI");
    }
    public StoragePluginId getId() {
      throw new UnsupportedOperationException("NYI");
    }
    @Deprecated // Remove this method as the namespace should keep track of views.
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      throw new UnsupportedOperationException("NYI");
    }
    public Class<? extends StoragePluginInstanceRulesFactory> getRulesFactoryClass() {
      throw new UnsupportedOperationException("NYI");
    }
    public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig) throws Exception {
      return CheckResult.UNCHANGED;
    }
  }
}
