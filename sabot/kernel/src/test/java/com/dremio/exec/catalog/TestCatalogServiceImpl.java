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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Set;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.calcite.util.Util;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.exec.store.sys.store.provider.KVPersistentStoreProvider;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.SourceTableDefinition;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.dataset.proto.PhysicalDataset;
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.protostuff.ByteString;

/**
 * Unit tests for {@link CatalogServiceImpl}.
 */
public class TestCatalogServiceImpl {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCatalogServiceImpl.class);

  private static final String HOSTNAME = "localhost";
  private static final int THREAD_COUNT = 2;
  private static final long RESERVATION = 0;
  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static final int TIMEOUT = 0;

  private static MockUpPlugin mockUpPlugin;

  private KVStoreProvider storeProvider;
  private NamespaceService namespaceService;
  private DatasetListingService datasetListingService;
  private BufferAllocator allocator;
  private LocalClusterCoordinator clusterCoordinator;
  private CloseableThreadPool pool;
  private FabricService fabricService;
  private NamespaceKey mockUpKey;
  private CatalogService catalogService;

  private final List<SourceTableDefinition> mockDatasets =
      ImmutableList.of(
          newDataset(MOCK_UP + ".fld1.ds11"),
          newDataset(MOCK_UP + ".fld1.ds12"),
          newDataset(MOCK_UP + ".fld2.fld21.ds211"),
          newDataset(MOCK_UP + ".fld2.ds22"),
          newDataset(MOCK_UP + ".ds3")
      );

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

    datasetListingService = new DatasetListingServiceImpl(DirectProvider.wrap(userName -> namespaceService));
    when(sabotContext.getDatasetListing())
        .thenReturn(datasetListingService);

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
    when(sabotContext.isCoordinator())
        .thenReturn(true);

    pool = new CloseableThreadPool("catalog-test");
    fabricService = new FabricServiceImpl(HOSTNAME, 45678, true, THREAD_COUNT, allocator, RESERVATION, MAX_ALLOCATION,
        TIMEOUT, pool);

    catalogService = new CatalogServiceImpl(
        DirectProvider.wrap(sabotContext),
        DirectProvider.wrap((SchedulerService) new LocalSchedulerService(1)),
        DirectProvider.wrap(new SystemTablePluginConfigProvider()),
        DirectProvider.wrap(fabricService),
        DirectProvider.wrap(ConnectionReader.of(sabotContext.getClasspathScan(), sabotConfig)));
    catalogService.start();

    mockUpPlugin = new MockUpPlugin();
    mockUpKey = new NamespaceKey(MOCK_UP);

    final SourceConfig mockUpConfig = new SourceConfig()
        .setName(MOCK_UP)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
        .setCtime(100L)
        .setConnectionConf(new MockUpConfig());

    doMockDatasets(mockUpPlugin, ImmutableList.<SourceTableDefinition>of());
    ((CatalogServiceImpl) catalogService).getSystemUserCatalog().createSource(mockUpConfig);
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(catalogService /* closes mockUpPlugin as well */, fabricService, pool, clusterCoordinator,
        allocator, storeProvider);
  }

  @Test
  public void refreshSourceMetadata_EmptySource() throws Exception {
    doMockDatasets(mockUpPlugin, ImmutableList.<SourceTableDefinition>of());
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    // make sure the namespace has no datasets under mockUpKey
    List<NamespaceKey> datasets = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(0, datasets.size());

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime() throws Exception {
    doMockDatasets(mockUpPlugin, mockDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    // make sure the namespace has datasets and folders according to the data supplied by plugin
    List<NamespaceKey> actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(mockDatasets, actualDatasetKeys);
    assertFoldersExist(Lists.newArrayList(MOCK_UP + ".fld1", MOCK_UP + ".fld2", MOCK_UP + ".fld2.fld21"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_UpdateWithNewDatasets() throws Exception {
    doMockDatasets(mockUpPlugin, mockDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    List<NamespaceKey> actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(5, actualDatasetKeys.size());

    List<SourceTableDefinition> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds13"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld21.ds212"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));

    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(9, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(Lists.newArrayList(MOCK_UP + ".fld1", MOCK_UP + ".fld2", MOCK_UP + ".fld2.fld21",
        MOCK_UP + ".fld5"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime_MultipleUpdatesWithNewDatasetsDeletedDatasets() throws Exception {
    doMockDatasets(mockUpPlugin, mockDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    List<SourceTableDefinition> testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds11"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld22.ds222"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds22"));
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));

    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(5, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(Lists.newArrayList(MOCK_UP + ".fld1", MOCK_UP + ".fld2", MOCK_UP + ".fld2.fld22",
        MOCK_UP + ".fld5"));
    assertFoldersDoNotExist(Lists.newArrayList(MOCK_UP + ".fld2.fld21"));
    assertDatasetSchemasDefined(actualDatasetKeys);

    // Make another refresh
    testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds11"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds22"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds23"));
    testDatasets.add(newDataset(MOCK_UP + ".ds5"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds52"));
    testDatasets.add(newDataset(MOCK_UP + ".fld6.ds61"));

    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogService.UpdateType.FULL);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(7, actualDatasetKeys.size());

    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(Lists.newArrayList(MOCK_UP + ".fld1", MOCK_UP + ".fld2", MOCK_UP + ".fld5", MOCK_UP + ".fld6"));
    assertFoldersDoNotExist(Lists.newArrayList((MOCK_UP + ".fld2.fld22")));
    assertDatasetSchemasDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  // Test whether name refresh will get new source dataset names, without refreshing the metadata for the datasets
  @Test
  public void refreshSourceNames() throws Exception {
    doMockDatasets(mockUpPlugin, mockDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.DEFAULT_METADATA_POLICY, CatalogService.UpdateType.NAMES);

    assertEquals(5, Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey)).size());

    List<SourceTableDefinition> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds13"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld21.ds212"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds23"));
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));
    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.DEFAULT_METADATA_POLICY, CatalogService.UpdateType.NAMES);

    // make sure the namespace has datasets and folders according to the data supplied by plugin in second request
    List<NamespaceKey> actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(10, actualDatasetKeys.size());
    assertDatasetsAreEqual(testDatasets, actualDatasetKeys);
    assertFoldersExist(Lists.newArrayList(MOCK_UP + ".fld1", MOCK_UP + ".fld2", MOCK_UP + ".fld2.fld21",
        MOCK_UP + ".fld5"));
    assertDatasetSchemasNotDefined(actualDatasetKeys);

    assertNoDatasetsAfterSourceDeletion();
  }

  // Test whether a test that failed to start will restart itself
  @Test
  public void testRemoveSourceAtFailedStart() throws Exception {
    String pluginName = MOCK_UP + "testFixFailedStart";
    mockUpPlugin.setThrowAtStart();

    MetadataPolicy rapidRefreshPolicy = new MetadataPolicy()
      .setAuthTtlMs(1L)
      .setDatasetUpdateMode(UpdateMode.PREFETCH)
      .setNamesRefreshMs(1L)
      .setDatasetDefinitionRefreshAfterMs(1L)
      .setDatasetDefinitionExpireAfterMs(1L);

    final SourceConfig mockUpConfig = new SourceConfig()
      .setName(pluginName)
      .setMetadataPolicy(rapidRefreshPolicy)
      .setCtime(100L)
      .setConnectionConf(new MockUpConfig());

    boolean testPassed = false;
    try {
      ((CatalogServiceImpl) catalogService).getSystemUserCatalog().createSource(mockUpConfig);
    } catch (UserException ue) {
      assertEquals(UserBitShared.DremioPBError.ErrorType.RESOURCE, ue.getErrorType());
      ManagedStoragePlugin msp = ((CatalogServiceImpl)catalogService).getManagedSource(pluginName);
      assertEquals(null, msp);
      testPassed = true;
    }
    assertTrue(testPassed);
  }

  @Test
  public void testConcurrencyErrors() throws Exception {
    // different etag
    SourceConfig mockUpConfig = new SourceConfig()
      .setName(MOCK_UP)
      .setCtime(100L)
      .setTag("4")
      .setConfigOrdinal(0L)
      .setConnectionConf(new MockUpConfig());

    boolean testPassed = false;
    try {
      ((CatalogServiceImpl) catalogService).getSystemUserCatalog().deleteSource(mockUpConfig);
    } catch (UserException ue) {
      testPassed = true;
    }
    assertTrue(testPassed);

    // different version
    mockUpConfig = new SourceConfig()
      .setName(MOCK_UP)
      .setCtime(100L)
      .setTag("0")
      .setConfigOrdinal(2L)
      .setConnectionConf(new MockUpConfig());

    testPassed = false;
    try {
      ((CatalogServiceImpl) catalogService).getSystemUserCatalog().deleteSource(mockUpConfig);
    } catch (UserException ue) {
      testPassed = true;
    }
    assertTrue(testPassed);
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
    } catch (Exception ignored) {
    }

    when(ret.getType()).thenReturn(DatasetType.PHYSICAL_DATASET_SOURCE_FILE);

    when(ret.isSaveable()).thenReturn(true);
    return ret;
  }

  private void doMockDatasets(StoragePlugin plugin, final List<SourceTableDefinition> datasets) throws Exception {
    ((MockUpPlugin) plugin).setDatasets(datasets);
  }

  private static void assertDatasetsAreEqual(List<SourceTableDefinition> expDatasets, List<NamespaceKey> actualDatasetKeys) {
    final Set<NamespaceKey> expDatasetKeys = FluentIterable.from(expDatasets)
        .transform(new Function<SourceTableDefinition, NamespaceKey>() {
          @Override
          public NamespaceKey apply(SourceTableDefinition input) {
            return input.getName();
          }
        }).toSet();
    assertEquals(expDatasetKeys, Sets.newHashSet(actualDatasetKeys));
  }

  private void assertFoldersExist(List<String> expFolders) throws Exception {
    for (String folderPath : expFolders) {
      NamespaceKey folderKey = new NamespaceKey(SqlUtils.parseSchemaPath(folderPath));
      namespaceService.getFolder(folderKey); // if the folder doesn't exit we get an exception
    }
  }

  private void assertFoldersDoNotExist(List<String> expFolders) throws Exception {
    for (String folderPath : expFolders) {
      NamespaceKey folderKey = new NamespaceKey(SqlUtils.parseSchemaPath(folderPath));
      try {
        namespaceService.getFolder(folderKey); // if the folder doesn't exit we get an exception
        fail();
      } catch (NamespaceNotFoundException ex) { /* no-op */ }
    }
  }

  private void assertDatasetSchemasDefined(List<NamespaceKey> datasetKeys) throws Exception {
    for (NamespaceKey datasetKey : datasetKeys) {
      DatasetConfig dsConfig = namespaceService.getDataset(datasetKey);
      BatchSchema schema = BatchSchema.deserialize(dsConfig.getRecordSchema());
      assertEquals(schema.getFieldCount(), 1);
      assertEquals(schema.getColumn(0).getName(), "string");
      assertEquals(schema.getColumn(0).getType(), ArrowType.Utf8.INSTANCE);
    }
  }

  private void assertDatasetSchemasNotDefined(List<NamespaceKey> datasetKeys) throws Exception {
    for (NamespaceKey datasetKey : datasetKeys) {
      DatasetConfig dsConfig = namespaceService.getDataset(datasetKey);
      assertEquals(dsConfig.getRecordSchema(), null);
    }
  }

  private void assertNoDatasetsAfterSourceDeletion() throws Exception {
    final SourceConfig sourceConfig = namespaceService.getSource(mockUpKey);
    namespaceService.deleteSource(mockUpKey, sourceConfig.getTag());

    assertEquals(0, Iterables.size(namespaceService.getAllDatasets(mockUpKey)));
  }

  private static final String MOCK_UP = "mockup";

  @SourceType(value = MOCK_UP, configurable = false)
  public static class MockUpConfig extends ConnectionConf<MockUpConfig, MockUpPlugin> {

    @Override
    public MockUpPlugin newPlugin(SabotContext context, String name,
                                  Provider<StoragePluginId> pluginIdProvider) {
      return mockUpPlugin;
    }
  }

  public static class MockUpPlugin implements StoragePlugin {
    private List<SourceTableDefinition> datasets;
    boolean throwAtStart = false;

    public void setDatasets(List<SourceTableDefinition> datasets) {
      this.datasets = datasets;
    }

    @Override
    public Iterable<SourceTableDefinition> getDatasets(String user, DatasetRetrievalOptions retrievalOptions) {
      return datasets;
    }

    @Override
    public SourceTableDefinition getDataset(NamespaceKey datasetPath, DatasetConfig oldDataset,
                                            DatasetRetrievalOptions retrievalOptions) {
      for (SourceTableDefinition definition : datasets) {
        if (Objects.equal(datasetPath, definition.getName())) {
          return definition;
        }
      }
      return null;
    }

    @Override
    public boolean containerExists(NamespaceKey key) {
      return false;
    }

    @Override
    public boolean datasetExists(NamespaceKey key) {
      for (SourceTableDefinition definition : datasets) {
        if (definition.getName().equals(key)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
    }

    @Override
    public SourceState getState() {
      return SourceState.goodState();
    }

    @Override
    public SourceCapabilities getSourceCapabilities() {
      return SourceCapabilities.NONE;
    }

    @Override
    public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
      return null;
    }

    @Override
    public CheckResult checkReadSignature(ByteString key, DatasetConfig datasetConfig, DatasetRetrievalOptions retrievalOptions) {
      return CheckResult.UNCHANGED;
    }

    @Override
    public void close() {
    }

    @Override
    public void start() {
      if (throwAtStart) {
        throw UserException.resourceError().build(logger);
      }
    }

    public void setThrowAtStart() {
      throwAtStart = true;
    }
    public void unsetThrowAtStart() {
      throwAtStart = false;
    }
  }
}
