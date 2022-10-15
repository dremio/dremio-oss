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

import static com.dremio.test.DremioTest.CLASSPATH_SCAN_RESULT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.AutoCloseableLock;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.SqlUtils;
import com.dremio.config.DremioConfig;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.planner.cost.ScanCostFactor;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.DefaultOptionManager;
import com.dremio.exec.server.options.OptionManagerWrapper;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.MissingPluginConf;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceNotFoundException;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceService.Factory;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.ModifiableLocalSchedulerService;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Unit tests for {@link CatalogServiceImpl}.
 *
 * NOTE: MetadataSynchronizer does not support prefetch for new datasets. So metadata is refreshed twice in some cases.
 * First time to add the names, and the second time to force refresh.
 */
public class TestCatalogServiceImpl {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TestCatalogServiceImpl.class);

  private static final String HOSTNAME = "localhost";
  private static final int THREAD_COUNT = 2;
  private static final long RESERVATION = 0;
  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static final int TIMEOUT = 0;
  private static final String MISSING_CONFIG_NAME = "MISSING_CONFIG";

  private static MockUpPlugin mockUpPlugin;
  private static MockUpBadPlugin mockUpBadPlugin;

  private LegacyKVStoreProvider storeProvider;
  private KVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;
  private Orphanage orphanage;
  private DatasetListingService datasetListingService;
  private BufferAllocator allocator;
  private LocalClusterCoordinator clusterCoordinator;
  private CloseableThreadPool pool;
  private FabricService fabricService;
  private NamespaceKey mockUpKey;
  private NamespaceKey mockUpBadKey;
  private CatalogServiceImpl catalogService;
  private String originalCatalogVersion;

  @Rule
  public TemporarySystemProperties properties = new TemporarySystemProperties();

  private final List<DatasetHandle> mockDatasets =
      ImmutableList.of(
          newDataset(MOCK_UP + ".fld1.ds11"),
          newDataset(MOCK_UP + ".fld1.ds12"),
          newDataset(MOCK_UP + ".fld2.fld21.ds211"),
          newDataset(MOCK_UP + ".fld2.ds22"),
          newDataset(MOCK_UP + ".ds3")
      );

  @Before
  public void setup() throws Exception {
    properties.set("dremio_masterless", "false");
    final SabotConfig sabotConfig = SabotConfig.create();
    final DremioConfig dremioConfig = DremioConfig.create();

    final SabotContext sabotContext = mock(SabotContext.class);

    storeProvider =
      LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    storeProvider.start();
    namespaceService = new NamespaceServiceImpl(storeProvider);

    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    orphanage = new OrphanageImpl(kvStoreProvider);

    final Orphanage.Factory orphanageFactory = new Orphanage.Factory() {
      @Override
      public Orphanage get() {
        return orphanage;
      }
    };

    final NamespaceService.Factory namespaceServiceFactory = new Factory() {
      @Override
      public NamespaceService get(String userName) {
        return namespaceService;
      }

      @Override
      public NamespaceService get(NamespaceIdentity identity) {
        return namespaceService;
      }
    };

    final ViewCreatorFactory viewCreatorFactory = new ViewCreatorFactory() {
      @Override
      public ViewCreator get(String userName) {
        return mock(ViewCreator.class);
      }

      @Override
      public void start() throws Exception {
      }

      @Override
      public void close() throws Exception {
      }
    };
    when(sabotContext.getNamespaceServiceFactory())
        .thenReturn(namespaceServiceFactory);
    when(sabotContext.getNamespaceService(anyString()))
        .thenReturn(namespaceService);
    when(sabotContext.getOrphanageFactory())
      .thenReturn(orphanageFactory);
    when(sabotContext.getViewCreatorFactoryProvider())
        .thenReturn(() -> viewCreatorFactory);

    datasetListingService = new DatasetListingServiceImpl(DirectProvider.wrap(namespaceServiceFactory));
    when(sabotContext.getDatasetListing())
        .thenReturn(datasetListingService);

    when(sabotContext.getClasspathScan())
        .thenReturn(CLASSPATH_SCAN_RESULT);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(sabotConfig, CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence())
        .thenReturn(lpp);
    final OptionValidatorListing optionValidatorListing = new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    final SystemOptionManager som = new SystemOptionManager(optionValidatorListing, lpp, () -> storeProvider, true);
    OptionManager optionManager = OptionManagerWrapper.Builder.newBuilder()
      .withOptionManager(new DefaultOptionManager(optionValidatorListing))
      .withOptionManager(som)
      .build();

    som.start();
    when(sabotContext.getOptionManager())
        .thenReturn(optionManager);

    when(sabotContext.getKVStoreProvider())
        .thenReturn(storeProvider);
    when(sabotContext.getConfig())
        .thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);
    when(sabotContext.getDremioConfig())
      .thenReturn(dremioConfig);

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

    when(sabotContext.getCredentialsServiceProvider())
      .thenReturn(() -> mock(CredentialsService.class));

    pool = new CloseableThreadPool("catalog-test");
    fabricService = new FabricServiceImpl(HOSTNAME, 45678, true, THREAD_COUNT, allocator, RESERVATION, MAX_ALLOCATION,
        TIMEOUT, pool);

    final MetadataRefreshInfoBroadcaster broadcaster = mock(MetadataRefreshInfoBroadcaster.class);
    doNothing().when(broadcaster).communicateChange(any());

    catalogService = new CatalogServiceImpl(
        () -> sabotContext,
        () -> new LocalSchedulerService(1),
        () -> new SystemTablePluginConfigProvider(),
        null,
        () -> fabricService,
        () -> ConnectionReader.of(sabotContext.getClasspathScan(), sabotConfig),
        () -> allocator,
        () -> storeProvider,
        () -> datasetListingService,
        () -> optionManager,
        () -> broadcaster,
        dremioConfig,
        EnumSet.allOf(ClusterCoordinator.Role.class),
        () -> new ModifiableLocalSchedulerService(2, "modifiable-scheduler-",
          ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES, () -> optionManager)
    );
    catalogService.start();

    mockUpPlugin = new MockUpPlugin();
    mockUpKey = new NamespaceKey(MOCK_UP);

    mockUpBadPlugin = new MockUpBadPlugin();
    mockUpBadKey = new NamespaceKey(MOCK_UP_BAD);

    final SourceConfig mockUpConfig = new SourceConfig()
        .setName(MOCK_UP)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
        .setCtime(100L)
        .setConnectionConf(new MockUpConfig());

    doMockDatasets(mockUpPlugin, ImmutableList.of());

    catalogService.getSystemUserCatalog().createSource(mockUpConfig);
    originalCatalogVersion = mockUpConfig.getTag();

    final SourceConfig mockUpBadConfig = new SourceConfig()
      .setName(MOCK_UP_BAD)
      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY)
      .setCtime(100L)
      .setConnectionConf(new MockUpBadConfig());
    catalogService.getSystemUserCatalog().createSource(mockUpBadConfig);
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(catalogService /* closes mockUpPlugin as well */, fabricService, pool, clusterCoordinator,
        allocator, storeProvider);
  }

  @Test
  public void refreshSourceMetadata_EmptySource() throws Exception {
    doMockDatasets(mockUpPlugin, ImmutableList.of());
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

    // make sure the namespace has no datasets under mockUpKey
    List<NamespaceKey> datasets = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(0, datasets.size());

    assertNoDatasetsAfterSourceDeletion();
  }

  @Test
  public void refreshSourceMetadata_FirstTime() throws Exception {
    doMockDatasets(mockUpPlugin, mockDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

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
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

    List<NamespaceKey> actualDatasetKeys = Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey));
    assertEquals(5, actualDatasetKeys.size());

    List<DatasetHandle> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds13"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld21.ds212"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));

    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

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
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

    List<DatasetHandle> testDatasets = Lists.newArrayList();
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds11"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld22.ds222"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds22"));
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));

    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

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
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);
    catalogService.refreshSource(mockUpKey, CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL);

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
    catalogService.refreshSource(mockUpKey, CatalogService.DEFAULT_METADATA_POLICY, CatalogServiceImpl.UpdateType.NAMES);

    assertEquals(5, Lists.newArrayList(namespaceService.getAllDatasets(mockUpKey)).size());

    List<DatasetHandle> testDatasets = Lists.newArrayList(mockDatasets);
    testDatasets.add(newDataset(MOCK_UP + ".fld1.ds13"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.fld21.ds212"));
    testDatasets.add(newDataset(MOCK_UP + ".fld2.ds23"));
    testDatasets.add(newDataset(MOCK_UP + ".ds4"));
    testDatasets.add(newDataset(MOCK_UP + ".fld5.ds51"));
    doMockDatasets(mockUpPlugin, testDatasets);
    catalogService.refreshSource(mockUpKey, CatalogService.DEFAULT_METADATA_POLICY, CatalogServiceImpl.UpdateType.NAMES);

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
      catalogService.getSystemUserCatalog().createSource(mockUpConfig);
    } catch (UserException ue) {
      assertEquals(UserBitShared.DremioPBError.ErrorType.RESOURCE, ue.getErrorType());
      ManagedStoragePlugin msp = catalogService.getManagedSource(pluginName);
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
      catalogService.getSystemUserCatalog().deleteSource(mockUpConfig);
    } catch (UserException ue) {
      testPassed = true;
    }
    assertTrue(testPassed);

    // different version
    mockUpConfig = new SourceConfig()
        .setName(MOCK_UP)
        .setCtime(100L)
        .setTag(originalCatalogVersion)
        .setConfigOrdinal(2L)
        .setConnectionConf(new MockUpConfig());

    testPassed = false;
    try {
      catalogService.getSystemUserCatalog().deleteSource(mockUpConfig);
    } catch (UserException ue) {
      testPassed = true;
    }
    assertTrue(testPassed);
  }

  @Test
  public void testDeleteMissingPlugin() {
    final MissingPluginConf missing = new MissingPluginConf();
    missing.throwOnInvocation = false;
    SourceConfig missingConfig = new SourceConfig()
      .setName(MISSING_CONFIG_NAME)
      .setConnectionConf(missing);

    catalogService.getSystemUserCatalog().createSource(missingConfig);
    catalogService.deleteSource(MISSING_CONFIG_NAME);

    // Check nullity of the state to confirm it's been deleted.
    assertNull(catalogService.getSourceState(MISSING_CONFIG_NAME));

  }

  @Test
  public void refreshMissingPlugin() throws Exception {
    final MissingPluginConf missing = new MissingPluginConf();
    missing.throwOnInvocation = false;
    SourceConfig missingConfig = new SourceConfig()
      .setName(MISSING_CONFIG_NAME)
      .setConnectionConf(missing);

    catalogService.getSystemUserCatalog().createSource(missingConfig);

    assertFalse(catalogService.refreshSource(new NamespaceKey(MISSING_CONFIG_NAME),
      CatalogService.REFRESH_EVERYTHING_NOW, CatalogServiceImpl.UpdateType.FULL));
  }

  @Test
  public void badSourceShouldNotBlockStorageRules() throws Exception {
    OptimizerRulesContext mock = mock(OptimizerRulesContext.class);

    ManagedStoragePlugin managedStoragePlugin = catalogService.getPlugins().get(MOCK_UP_BAD);
    managedStoragePlugin.refreshState().get();

    AtomicBoolean test = new AtomicBoolean(false);

    Runnable runnable = () -> {
      catalogService.getStorageRules(mock, PlannerPhase.RELATIONAL_PLANNING);
      test.set(true);
    };

    Thread thread = new Thread(runnable);
    // we get the writelock and run code in a different thread that should not use a readlock
    try (AutoCloseableLock unused = managedStoragePlugin.writeLock()) {
      thread.start();
      thread.join(1000);
    }
    thread.interrupt();
    assertTrue(test.get());
  }

  private abstract static class DatasetImpl implements DatasetTypeHandle, DatasetMetadata, PartitionChunkListing {
  }

  private static DatasetImpl newDataset(final String dsPath) {
    return new DatasetImpl() {
      @Override
      public Iterator<? extends PartitionChunk> iterator() {
        return Collections.singleton(PartitionChunk.of(DatasetSplit.of(0,0))).iterator();
      }

      @Override
      public DatasetType getDatasetType() {
        return DatasetType.PHYSICAL_DATASET_SOURCE_FILE;
      }

      @Override
      public EntityPath getDatasetPath() {
        return new EntityPath(SqlUtils.parseSchemaPath(dsPath));
      }

      @Override
      public DatasetStats getDatasetStats() {
        return DatasetStats.of(0, ScanCostFactor.OTHER.getFactor());
      }

      @Override
      public Schema getRecordSchema() {
        return BatchSchema.newBuilder()
            .addField(new Field("string", FieldType.nullable(ArrowType.Utf8.INSTANCE), null))
            .build();
      }
    };
  }

  private void doMockDatasets(StoragePlugin plugin, final List<DatasetHandle> datasets) throws Exception {
    ((MockUpPlugin) plugin).setDatasets(datasets);
  }

  private static void assertDatasetsAreEqual(List<DatasetHandle> expDatasets, List<NamespaceKey> actualDatasetKeys) {
    final Set<NamespaceKey> expDatasetKeys = expDatasets.stream()
        .map(input -> new NamespaceKey(input.getDatasetPath().getComponents()))
        .collect(Collectors.toSet());
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

  static final String MOCK_UP = "mockup";
  private static final String MOCK_UP_BAD = "mockup_bad";

  @SourceType(value = MOCK_UP, configurable = false)
  public static class MockUpConfig extends ConnectionConf<MockUpConfig, MockUpPlugin> {
    MockUpPlugin plugin;

    MockUpConfig() {
      this(mockUpPlugin);
    }

    MockUpConfig(MockUpPlugin plugin) {
      this.plugin = plugin;
    }

    @Override
    public MockUpPlugin newPlugin(SabotContext context, String name,
                                  Provider<StoragePluginId> pluginIdProvider) {
      return plugin;
    }
  }

  public static class MockUpPlugin implements ExtendedStoragePlugin {
    private List<DatasetHandle> datasets;
    boolean throwAtStart = false;

    public void setDatasets(List<DatasetHandle> datasets) {
      this.datasets = datasets;
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

    @Override
    public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
      return () -> datasets.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
      return datasets.stream()
          .filter(dataset -> dataset.getDatasetPath().equals(datasetPath))
          .findFirst();
    }

    @Override
    public DatasetMetadata getDatasetMetadata(
        DatasetHandle datasetHandle,
        PartitionChunkListing chunkListing,
        GetMetadataOption... options
    ) {
      return datasetHandle.unwrap(DatasetImpl.class);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
      return datasetHandle.unwrap(DatasetImpl.class);
    }

    @Override
    public boolean containerExists(EntityPath containerPath) {
      return false;
    }

    @Override
    public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) {
      return BytesOutput.NONE;
    }

    @Override
    public MetadataValidity validateMetadata(
        BytesOutput signature,
        DatasetHandle datasetHandle,
        DatasetMetadata metadata,
        ValidateMetadataOption... options
    ) {
      return MetadataValidity.INVALID;
    }
  }

  @SourceType(value = MOCK_UP_BAD, configurable = false)
  public static class MockUpBadConfig extends ConnectionConf<MockUpBadConfig, MockUpBadPlugin> {

    @Override
    public MockUpBadPlugin newPlugin(SabotContext context, String name,
                                  Provider<StoragePluginId> pluginIdProvider) {
      return mockUpBadPlugin;
    }
  }

  public static class MockUpBadPlugin implements ExtendedStoragePlugin {
    private List<DatasetHandle> datasets;
    boolean throwAtStart = false;
    boolean goodAtStart = true;

    public void setDatasets(List<DatasetHandle> datasets) {
      this.datasets = datasets;
    }

    @Override
    public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
      return true;
    }

    @Override
    public SourceState getState() {
      if (goodAtStart) {
        goodAtStart = false;
        return SourceState.goodState();
      }
      return SourceState.badState("");
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

    @Override
    public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
      return () -> datasets.iterator();
    }

    @Override
    public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) {
      return datasets.stream()
        .filter(dataset -> dataset.getDatasetPath().equals(datasetPath))
        .findFirst();
    }

    @Override
    public DatasetMetadata getDatasetMetadata(
      DatasetHandle datasetHandle,
      PartitionChunkListing chunkListing,
      GetMetadataOption... options
    ) {
      return datasetHandle.unwrap(DatasetImpl.class);
    }

    @Override
    public PartitionChunkListing listPartitionChunks(DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
      return datasetHandle.unwrap(DatasetImpl.class);
    }

    @Override
    public boolean containerExists(EntityPath containerPath) {
      return false;
    }

    @Override
    public BytesOutput provideSignature(DatasetHandle datasetHandle, DatasetMetadata metadata) {
      return BytesOutput.NONE;
    }

    @Override
    public MetadataValidity validateMetadata(
      BytesOutput signature,
      DatasetHandle datasetHandle,
      DatasetMetadata metadata,
      ValidateMetadataOption... options
    ) {
      return MetadataValidity.INVALID;
    }
  }
}
