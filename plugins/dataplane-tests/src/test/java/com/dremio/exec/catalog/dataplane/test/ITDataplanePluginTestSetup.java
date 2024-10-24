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
package com.dremio.exec.catalog.dataplane.test;

import static com.dremio.exec.ExecConstants.ENABLE_MERGE_BRANCH_BEHAVIOR;
import static com.dremio.exec.ExecConstants.SLICE_TARGET_OPTION;
import static com.dremio.exec.catalog.CatalogOptions.SUPPORT_V1_ICEBERG_VIEWS;
import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.ALTERNATE_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneStorage.BucketSelection.PRIMARY_BUCKET;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.NO_ANCESTOR;
import static com.dremio.exec.catalog.dataplane.test.DataplaneTestDefines.selectFileLocationsQuery;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AZURE_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_GCS_STORAGE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.dremio.catalog.model.VersionContext;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogOptions;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DatasetCatalogServiceImpl;
import com.dremio.exec.catalog.InformationSchemaServiceImpl;
import com.dremio.exec.catalog.MetadataRefreshInfoBroadcaster;
import com.dremio.exec.catalog.VersionedDatasetAdapterFactory;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.options.OptionManager;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.sysflight.SysFlightPluginConfigProvider;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.embedded.catalog.EmbeddedMetadataPointerService;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvents;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import java.io.File;
import java.net.URI;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.Tag;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;

/** Set up for DataplanePlugin integration tests */
@ExtendWith(MultipleDataplaneStorageExtension.class)
@ExtendWith(CurrentNessieServerExtension.class)
public abstract class ITDataplanePluginTestSetup extends DataplaneTestHelper {

  private static final org.slf4j.Logger LOGGER =
      org.slf4j.LoggerFactory.getLogger(ITDataplanePluginTestSetup.class);

  // Storage
  @PopulateDataplaneStorage private static DataplaneStorage dataplaneStorage;

  // Nessie
  @NessieBaseUri private static URI nessieUri;
  private static NessieApiV2 nessieApi;
  private static NessieCatalog nessieIcebergCatalog;

  // Dependencies
  private static Catalog catalog;
  private static NamespaceService namespaceService;

  // System under test
  private static DataplanePlugin dataplanePlugin;

  @BeforeAll
  static void setUp() throws Exception {
    setUpSabotNodeRule();
    setSystemOption(DATAPLANE_AZURE_STORAGE_ENABLED, true);
    setSystemOption(ENABLE_MERGE_BRANCH_BEHAVIOR, true);
    setSystemOption(DATAPLANE_GCS_STORAGE_ENABLED, true);
    setUpNessie();
    setUpDataplanePlugin();
    setUpNessieIcebergCatalog();

    // Set support key for V1 views to be enabled to mimic software support key being enabled
    // TODO: Will be removed DX-91353
    setSystemOption(SUPPORT_V1_ICEBERG_VIEWS, true);
  }

  @AfterAll
  static void tearDown() throws Exception {
    resetSessionSettings();
    AutoCloseables.close(nessieApi);
  }

  @BeforeEach
  void before() throws Exception {
    setDataplaneDefaultSessionSettings();
    Branch defaultBranch = getNessieApi().getDefaultBranch();
    getNessieApi().getAllReferences().stream()
        .forEach(
            ref -> {
              try {
                if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
                  getNessieApi().deleteBranch().branch((Branch) ref).delete();
                } else if (ref instanceof Tag) {
                  getNessieApi().deleteTag().tag((Tag) ref).delete();
                }
              } catch (NessieConflictException | NessieNotFoundException e) {
                throw new RuntimeException(e);
              }
            });

    getNessieApi().assignBranch().branch(defaultBranch).assignTo(Detached.of(NO_ANCESTOR)).assign();
  }

  protected static void setUpNessie() {
    nessieApi =
        NessieClientBuilder.createClientBuilder("HTTP", null)
            .withUri(createNessieURIString())
            .build(NessieApiV2.class);
  }

  private static void setUpSabotNodeRule() throws Exception {
    SABOT_NODE_RULE.register(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(SysFlightPluginConfigProvider.class)
                .toInstance(new SysFlightPluginConfigProvider());

            // For System Table set
            final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();

            final boolean isMaster = true;
            EmbeddedMetadataPointerService pointerService =
                new EmbeddedMetadataPointerService(getProvider(KVStoreProvider.class));
            pointerService.getGrpcServices().forEach(conduitServiceRegistry::registerService);

            final DatasetCatalogServiceImpl datasetCatalogServiceImpl =
                new DatasetCatalogServiceImpl(
                    getProvider(CatalogService.class), getProvider(NamespaceService.Factory.class));
            bind(DatasetCatalogServiceImpl.class).toInstance(datasetCatalogServiceImpl);
            conduitServiceRegistry.registerService(datasetCatalogServiceImpl);

            conduitServiceRegistry.registerService(
                new InformationSchemaServiceImpl(
                    getProvider(CatalogService.class),
                    () ->
                        new ContextMigratingExecutorService
                            .ContextMigratingCloseableExecutorService<>(
                            new CloseableThreadPool("DataplaneEnterpriseTestSetup-"))));

            bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);
            // End System Table set

            bind(ModifiableSchedulerService.class)
                .toInstance(mock(ModifiableSchedulerService.class));
            bind(CatalogService.class)
                .toInstance(
                    new CatalogServiceImpl(
                        getProvider(SabotContext.class),
                        getProvider(SchedulerService.class),
                        getProvider(SystemTablePluginConfigProvider.class),
                        getProvider(SysFlightPluginConfigProvider.class),
                        getProvider(FabricService.class),
                        getProvider(ConnectionReader.class),
                        getProvider(BufferAllocator.class),
                        getProvider(LegacyKVStoreProvider.class),
                        getProvider(DatasetListingService.class),
                        getProvider(OptionManager.class),
                        () -> mock(MetadataRefreshInfoBroadcaster.class),
                        mock(DremioConfig.class),
                        EnumSet.allOf(ClusterCoordinator.Role.class),
                        getProvider(ModifiableSchedulerService.class),
                        getProvider(VersionedDatasetAdapterFactory.class),
                        getProvider(CatalogStatusEvents.class),
                        () -> mock(ExecutorService.class)));
          }
        });
    setupDefaultTestCluster();
  }

  protected static NessiePluginConfig prepareConnectionConf(
      DataplaneStorage.BucketSelection bucketSelection) {
    return dataplaneStorage.prepareNessiePluginConfig(bucketSelection, createNessieURIString());
  }

  protected static void setUpDataplanePlugin() {
    LOGGER.info("Using Dataplane storage type: {}", dataplaneStorage.getType());

    SourceConfig sourceConfig =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(PRIMARY_BUCKET))
            .setName(DATAPLANE_PLUGIN_NAME)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalog = getCatalogService().getSystemUserCatalog();
    catalog.createSource(sourceConfig);
    dataplanePlugin = catalog.getSource(DATAPLANE_PLUGIN_NAME);

    namespaceService = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
  }

  public static void createSpaceInNamespace(String spaceNames) throws NamespaceException {
    namespaceService.addOrUpdateSpace(new NamespaceKey(spaceNames), new SpaceConfig());
  }

  public static void setupForAnotherPlugin() {
    SourceConfig sourceConfigForReflectionTest =
        new SourceConfig()
            .setConnectionConf(prepareConnectionConf(PRIMARY_BUCKET))
            .setName(DATAPLANE_PLUGIN_NAME_FOR_REFLECTION_TEST)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalog.createSource(sourceConfigForReflectionTest);
  }

  /** Defaulted to use DataplanePlugin Connection configuration if not passed */
  public static void setupForCreatingSources(
      Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf) {
    CatalogService catalogService = getCatalogService();
    sourceNamesWithConnectionConf.forEach(
        (sourceName, connectionConf) -> {
          if (catalogService
              .getAllVersionedPlugins()
              .noneMatch(s -> s.getName().equals(sourceName))) {
            SourceConfig sourceConfig;
            if (connectionConf != null) {
              sourceConfig =
                  new SourceConfig()
                      .setConnectionConf(connectionConf)
                      .setName(sourceName)
                      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
            } else {
              sourceConfig =
                  new SourceConfig()
                      .setConnectionConf(prepareConnectionConf(PRIMARY_BUCKET))
                      .setName(sourceName)
                      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
            }
            catalogService.getSystemUserCatalog().createSource(sourceConfig);
          }
        });
  }

  /** Defaulted to use DataplanePlugin Connection configuration if not passed */
  public static void cleanupForDeletingSources(
      Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf) {
    CatalogService catalogService = getCatalogService();
    sourceNamesWithConnectionConf.forEach(
        (sourceName, connectionConf) -> {
          if (catalogService
              .getAllVersionedPlugins()
              .anyMatch(s -> s.getName().equals(sourceName))) {
            SourceConfig sourceConfig;
            if (connectionConf != null) {
              sourceConfig =
                  new SourceConfig()
                      .setConnectionConf(connectionConf)
                      .setName(sourceName)
                      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
            } else {
              sourceConfig =
                  new SourceConfig()
                      .setConnectionConf(prepareConnectionConf(PRIMARY_BUCKET))
                      .setName(sourceName)
                      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
            }
            catalogService.getSystemUserCatalog().deleteSource(sourceConfig);
          }
        });
  }

  protected static void setUpNessieIcebergCatalog() throws NessieNotFoundException {
    nessieIcebergCatalog = new NessieCatalog();
    Branch defaultRef = nessieApi.getDefaultBranch();

    NessieIcebergClient nessieIcebergClient =
        new NessieIcebergClient(
            nessieApi, defaultRef.getName(), defaultRef.getHash(), new HashMap<>());

    nessieIcebergCatalog.initialize(
        "test",
        nessieIcebergClient,
        dataplaneStorage.getFileIO(),
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, dataplaneStorage.getWarehousePath()));
  }

  public Table loadTable(String table) {
    return nessieIcebergCatalog.loadTable(TableIdentifier.parse(table));
  }

  public Table createTable(String table, Map<String, String> properties) {
    Schema icebergTableSchema =
        new Schema(ImmutableList.of(Types.NestedField.required(0, "c1", new Types.IntegerType())));
    TableIdentifier tableId = TableIdentifier.parse(table);
    String location = String.format("%s/%s", dataplaneStorage.getWarehousePath(), table);
    return nessieIcebergCatalog.createTable(
        tableId, icebergTableSchema, PartitionSpec.unpartitioned(), location, properties);
  }

  public void runWithAlternateSourcePath(String sql) throws Exception {
    try (AutoCloseable ignored = () -> changeSourceRootPath(PRIMARY_BUCKET)) {
      changeSourceRootPath(ALTERNATE_BUCKET);
      runSQL(sql);
    }
  }

  private static void changeSourceRootPath(DataplaneStorage.BucketSelection bucketSelection)
      throws NamespaceException {
    SourceConfig sourceConfig = namespaceService.getSource(new NamespaceKey(DATAPLANE_PLUGIN_NAME));
    sourceConfig.setConnectionConf(prepareConnectionConf(bucketSelection));
    Catalog systemUserCatalog = getCatalogService().getSystemUserCatalog();
    systemUserCatalog.updateSource(sourceConfig);

    // get ref to new DataplanePlugin (old one was closed during ManagedStoragePlugin.replacePlugin)
    dataplanePlugin = systemUserCatalog.getSource(DATAPLANE_PLUGIN_NAME);
  }

  @Override
  public NessieApiV2 getNessieApi() {
    return nessieApi;
  }

  @Override
  public DataplaneStorage getDataplaneStorage() {
    return dataplaneStorage;
  }

  public DataplanePlugin getDataplanePlugin() {
    return dataplanePlugin;
  }

  public Catalog getContextualizedCatalog(String pluginName, VersionContext versionContext) {
    Catalog resetCatalog =
        getCatalog().resolveCatalogResetContext(DATAPLANE_PLUGIN_NAME, versionContext);
    Catalog newCatalog =
        resetCatalog.resolveCatalog(ImmutableMap.of(DATAPLANE_PLUGIN_NAME, versionContext));
    return newCatalog;
  }

  public Catalog getCatalog() {
    return catalog;
  }

  public void assertAllFilesAreInBucket(
      List<String> tablePath, DataplaneStorage.BucketSelection bucketSelection) throws Exception {
    List<String> fileLocations = runSqlWithResults(selectFileLocationsQuery(tablePath)).get(0);
    assertThat(fileLocations)
        .allMatch(fileLocation -> dataplaneStorage.doesObjectExist(bucketSelection, fileLocation));
  }

  protected static File createTempLocation() {
    String locationName = RandomStringUtils.randomAlphanumeric(8);
    File location = new File(getDfsTestTmpSchemaLocation(), locationName);
    location.mkdirs();
    return location;
  }

  /* Helper function used to set up the NessieURI as a string. */
  protected static String createNessieURIString() {
    return nessieUri.resolve("api/v2").toString();
  }

  public void setNessieGCDefaultCutOffPolicy(String value) {
    ImmutableGarbageCollectorConfig gcConfig =
        ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy(value).build();
    try {
      getNessieApi().updateRepositoryConfig().repositoryConfig(gcConfig).update();
    } catch (NessieConflictException e) {
      throw new RuntimeException("Failed to update Nessie configs", e);
    }
  }

  public void setSliceTarget(long target) {
    setSystemOption(ExecConstants.SLICE_TARGET_OPTION, target);
  }

  public void resetSliceTarget() {
    resetSystemOption(SLICE_TARGET_OPTION);
  }

  public void setTargetBatchSize(int batchSize) {
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN, 1);
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, batchSize);
  }

  public void resetTargetBatchSize() {
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN);
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX);
  }

  public void setDataplaneDefaultSessionSettings() throws Exception {
    resetSessionSettings();
    setSystemOption(DATAPLANE_AZURE_STORAGE_ENABLED, true);
  }

  public static AutoCloseable enableVersionedSourceUdf() {
    return withSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED, true);
  }

  public static AutoCloseable disableVersionedSourceUdf() {
    return withSystemOption(CatalogOptions.VERSIONED_SOURCE_UDF_ENABLED, false);
  }
}
