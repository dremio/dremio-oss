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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.LocalKVStoreProvider;
import com.dremio.datastore.adapter.LegacyKVStoreProviderAdapter;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.conf.ConnectionConf;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.OptionValidatorListingImpl;
import com.dremio.exec.server.options.SystemOptionManager;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.options.impl.DefaultOptionManager;
import com.dremio.options.impl.OptionManagerWrapper;
import com.dremio.service.DirectProvider;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.coordinator.local.LocalClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.listing.DatasetListingServiceImpl;
import com.dremio.service.namespace.NamespaceIdentity;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.NamespaceServiceImpl;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventsImpl;
import com.dremio.service.namespace.source.proto.MetadataPolicy;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.source.proto.UpdateMode;
import com.dremio.service.orphanage.Orphanage;
import com.dremio.service.orphanage.OrphanageImpl;
import com.dremio.service.scheduler.LocalSchedulerService;
import com.dremio.service.scheduler.ModifiableWrappedSchedulerService;
import com.dremio.services.credentials.CredentialsService;
import com.dremio.services.credentials.SecretsCreator;
import com.dremio.services.fabric.FabricServiceImpl;
import com.dremio.services.fabric.api.FabricService;
import com.dremio.test.DremioTest;
import com.dremio.test.TemporarySystemProperties;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import javax.inject.Provider;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

/** Unit tests for {@link CatalogServiceImpl} with dremio_masterless enabled */
public class TestMasterLessCatalogServiceImpl {

  private static final String HOSTNAME = "localhost";
  private static final int THREAD_COUNT = 2;
  private static final long RESERVATION = 0;
  private static final long MAX_ALLOCATION = Long.MAX_VALUE;
  private static final int TIMEOUT = 0;

  static final String MOCK_UP_MASTER_LESS = "mockup_masterless_source";

  @SourceType(value = MOCK_UP_MASTER_LESS, configurable = false)
  public static class MockUpConfig
      extends ConnectionConf<
          TestCatalogServiceImpl.MockUpConfig, TestCatalogServiceImpl.MockUpPlugin> {
    TestCatalogServiceImpl.MockUpPlugin plugin;

    MockUpConfig() {
      this(mockUpPlugin);
    }

    MockUpConfig(TestCatalogServiceImpl.MockUpPlugin plugin) {
      this.plugin = plugin;
    }

    @Override
    public TestCatalogServiceImpl.MockUpPlugin newPlugin(
        SabotContext context, String name, Provider<StoragePluginId> pluginIdProvider) {
      return plugin;
    }
  }

  private static TestCatalogServiceImpl.MockUpPlugin mockUpPlugin;

  private LegacyKVStoreProvider storeProvider;
  private KVStoreProvider kvStoreProvider;
  private NamespaceService namespaceService;
  private Orphanage orphanage;
  private DatasetListingService datasetListingService;
  private BufferAllocator allocator;
  private CloseableThreadPool pool;
  private FabricService fabricService;
  private CatalogServiceImpl catalogService;
  private LocalClusterCoordinator localClusterCoordinator;

  @Rule public TemporarySystemProperties properties = new TemporarySystemProperties();

  @Before
  public void setup() throws Exception {
    properties.set("dremio_masterless", "true");
    CoordinationProtos.NodeEndpoint nodeEndpoint2 =
        CoordinationProtos.NodeEndpoint.newBuilder()
            .setAddress("host2")
            .setFabricPort(1235)
            .setUserPort(2346)
            .setRoles(
                ClusterCoordinator.Role.toEndpointRoles(
                    Sets.newHashSet(ClusterCoordinator.Role.COORDINATOR)))
            .build();
    final SabotConfig sabotConfig = SabotConfig.create();
    final DremioConfig dremioConfig = DremioConfig.create();

    final SabotContext sabotContext = mock(SabotContext.class);

    storeProvider = LegacyKVStoreProviderAdapter.inMemory(DremioTest.CLASSPATH_SCAN_RESULT);
    storeProvider.start();
    namespaceService = new NamespaceServiceImpl(storeProvider, mock(CatalogStatusEvents.class));
    localClusterCoordinator = LocalClusterCoordinator.newRunningCoordinator();
    kvStoreProvider = new LocalKVStoreProvider(DremioTest.CLASSPATH_SCAN_RESULT, null, true, false);
    kvStoreProvider.start();
    orphanage = new OrphanageImpl(kvStoreProvider);

    final Orphanage.Factory orphanageFactory =
        new Orphanage.Factory() {
          @Override
          public Orphanage get() {
            return orphanage;
          }
        };

    final NamespaceService.Factory namespaceServiceFactory =
        new NamespaceService.Factory() {
          @Override
          public NamespaceService get(String userName) {
            return namespaceService;
          }

          @Override
          public NamespaceService get(NamespaceIdentity identity) {
            return namespaceService;
          }
        };

    final ViewCreatorFactory viewCreatorFactory =
        new ViewCreatorFactory() {
          @Override
          public ViewCreator get(String userName) {
            return mock(ViewCreator.class);
          }

          @Override
          public void start() throws Exception {}

          @Override
          public void close() throws Exception {}
        };
    when(sabotContext.getNamespaceServiceFactory()).thenReturn(namespaceServiceFactory);
    when(sabotContext.getNamespaceService(anyString())).thenReturn(namespaceService);
    when(sabotContext.getOrphanageFactory()).thenReturn(orphanageFactory);
    when(sabotContext.getViewCreatorFactoryProvider()).thenReturn(() -> viewCreatorFactory);

    datasetListingService =
        new DatasetListingServiceImpl(DirectProvider.wrap(namespaceServiceFactory));
    when(sabotContext.getDatasetListing()).thenReturn(datasetListingService);

    when(sabotContext.getClasspathScan()).thenReturn(CLASSPATH_SCAN_RESULT);

    final LogicalPlanPersistence lpp = new LogicalPlanPersistence(CLASSPATH_SCAN_RESULT);
    when(sabotContext.getLpPersistence()).thenReturn(lpp);
    final OptionValidatorListing optionValidatorListing =
        new OptionValidatorListingImpl(CLASSPATH_SCAN_RESULT);
    final SystemOptionManager som =
        new SystemOptionManager(optionValidatorListing, lpp, () -> storeProvider, true);
    OptionManager optionManager =
        OptionManagerWrapper.Builder.newBuilder()
            .withOptionManager(new DefaultOptionManager(optionValidatorListing))
            .withOptionManager(som)
            .build();

    som.start();
    when(sabotContext.getOptionManager()).thenReturn(optionManager);

    when(sabotContext.getKVStoreProvider()).thenReturn(storeProvider);
    when(sabotContext.getConfig()).thenReturn(DremioTest.DEFAULT_SABOT_CONFIG);
    when(sabotContext.getDremioConfig()).thenReturn(dremioConfig);

    allocator = RootAllocatorFactory.newRoot(sabotConfig);
    when(sabotContext.getAllocator()).thenReturn(allocator);

    when(sabotContext.getClusterCoordinator()).thenReturn(localClusterCoordinator);
    when(sabotContext.getExecutors())
        .thenReturn(
            localClusterCoordinator
                .getServiceSet(ClusterCoordinator.Role.EXECUTOR)
                .getAvailableEndpoints());
    when(sabotContext.getCoordinators())
        .thenReturn(
            localClusterCoordinator
                .getServiceSet(ClusterCoordinator.Role.COORDINATOR)
                .getAvailableEndpoints());

    when(sabotContext.getRoles())
        .thenReturn(
            Sets.newHashSet(ClusterCoordinator.Role.MASTER, ClusterCoordinator.Role.COORDINATOR));
    when(sabotContext.isCoordinator()).thenReturn(true);

    when(sabotContext.getCredentialsServiceProvider())
        .thenReturn(() -> mock(CredentialsService.class));

    when(sabotContext.getSecretsCreator()).thenReturn(() -> mock(SecretsCreator.class));

    pool = new CloseableThreadPool("catalog-test");
    fabricService =
        new FabricServiceImpl(
            HOSTNAME,
            45678,
            true,
            THREAD_COUNT,
            allocator,
            RESERVATION,
            MAX_ALLOCATION,
            TIMEOUT,
            pool);

    final MetadataRefreshInfoBroadcaster broadcaster = mock(MetadataRefreshInfoBroadcaster.class);
    doNothing().when(broadcaster).communicateChange(any());

    catalogService =
        new CatalogServiceImpl(
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
            () ->
                new ModifiableWrappedSchedulerService(
                    2,
                    true,
                    "modifiable-scheduler-",
                    () -> localClusterCoordinator,
                    () -> localClusterCoordinator,
                    () -> nodeEndpoint2,
                    ExecConstants.MAX_CONCURRENT_METADATA_REFRESHES,
                    () -> optionManager,
                    () -> null,
                    false),
            () -> new VersionedDatasetAdapterFactory(),
            () -> new CatalogStatusEventsImpl());
    catalogService.start();
    mockUpPlugin = new TestCatalogServiceImpl.MockUpPlugin();
  }

  @After
  public void shutdown() throws Exception {
    AutoCloseables.close(
        catalogService /* closes mockUpPlugin as well */,
        fabricService,
        pool,
        allocator,
        storeProvider);
  }

  // On source deletion, its data from ZK should also be deleted
  @Test
  public void testDeleteSourceInfoFromZkOnSourceDelete() throws Exception {
    String pluginName = MOCK_UP_MASTER_LESS + "testDeleteSourceInfoFromZkOnSourceDelete";
    String metadataRefreshTaskName = "metadata-refresh-" + pluginName;

    MetadataPolicy rapidRefreshPolicy =
        new MetadataPolicy()
            .setAuthTtlMs(1L)
            .setDatasetUpdateMode(UpdateMode.PREFETCH)
            .setNamesRefreshMs(1L)
            .setDatasetDefinitionRefreshAfterMs(1L)
            .setDatasetDefinitionExpireAfterMs(1L);
    final SourceConfig mockUpConfig =
        new SourceConfig()
            .setName(pluginName)
            .setMetadataPolicy(rapidRefreshPolicy)
            .setCtime(100L)
            .setConnectionConf(new TestMasterLessCatalogServiceImpl.MockUpConfig());

    catalogService.getSystemUserCatalog().createSource(mockUpConfig);
    Assert.assertNotNull(localClusterCoordinator.getServiceSet(metadataRefreshTaskName));
    catalogService.deleteSource(pluginName);
    Assert.assertNull(localClusterCoordinator.getServiceSet(metadataRefreshTaskName));
  }
}
