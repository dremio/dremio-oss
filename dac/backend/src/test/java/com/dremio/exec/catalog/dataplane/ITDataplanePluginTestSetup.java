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
package com.dremio.exec.catalog.dataplane;

import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_SORT_ORDER;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_TABLE_PROPERTIES;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_TIME_TRAVEL;
import static com.dremio.exec.ExecConstants.ENABLE_ICEBERG_VACUUM_CATALOG;
import static com.dremio.exec.ExecConstants.SLICE_TARGET_OPTION;
import static com.dremio.exec.ExecConstants.VERSIONED_VIEW_ENABLED;
import static com.dremio.exec.catalog.CatalogOptions.REFLECTION_VERSIONED_SOURCE_ENABLED;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.ALTERNATIVE_BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.BUCKET_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.DATAPLANE_PLUGIN_NAME;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.NO_ANCESTOR;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.S3_PREFIX;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.createFolderAtQueryWithIfNotExists;
import static com.dremio.exec.catalog.dataplane.DataplaneTestDefines.selectFileLocationsQuery;
import static com.dremio.exec.store.DataplanePluginOptions.NESSIE_PLUGIN_ENABLED;
import static com.dremio.options.OptionValue.OptionType.SYSTEM;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.nessie.NessieIcebergClient;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Detached;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.Tag;
import org.projectnessie.tools.compatibility.api.NessieBaseUri;
import org.projectnessie.tools.compatibility.api.NessieServerProperty;
import org.projectnessie.tools.compatibility.internal.OlderNessieServersExtension;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.dremio.BaseTestQuery;
import com.dremio.catalog.model.VersionContext;
import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.config.DremioConfig;
import com.dremio.datastore.api.KVStoreProvider;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.CatalogServiceImpl;
import com.dremio.exec.catalog.ConnectionReader;
import com.dremio.exec.catalog.DatasetCatalogServiceImpl;
import com.dremio.exec.catalog.InformationSchemaServiceImpl;
import com.dremio.exec.catalog.MetadataRefreshInfoBroadcaster;
import com.dremio.exec.catalog.VersionedDatasetAdapterFactory;
import com.dremio.exec.catalog.conf.NessieAuthType;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.sys.SystemTablePluginConfigProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.plugins.dataplane.store.DataplanePlugin;
import com.dremio.plugins.dataplane.store.NessiePluginConfig;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.plugins.sysflight.SysFlightPluginConfigProvider;
import com.dremio.service.conduit.server.ConduitServiceRegistry;
import com.dremio.service.conduit.server.ConduitServiceRegistryImpl;
import com.dremio.service.coordinator.ClusterCoordinator;
import com.dremio.service.listing.DatasetListingService;
import com.dremio.service.namespace.AbstractConnectionConf;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.source.proto.SourceConfig;
import com.dremio.service.nessie.NessieService;
import com.dremio.service.scheduler.ModifiableSchedulerService;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.users.SystemUser;
import com.dremio.services.fabric.api.FabricService;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;

import io.findify.s3mock.S3Mock;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Set up for DataplanePlugin integration tests
 */
@ExtendWith(OlderNessieServersExtension.class)
@NessieServerProperty(name = "nessie.test.storage.kind", value = "PERSIST")  //PERSIST is the new model in Nessie Server
public abstract class ITDataplanePluginTestSetup extends DataplaneTestHelper {

  private static AmazonS3 s3Client;
  private static S3Mock s3Mock;
  private static int s3Port;

  @TempDir
  static File temporaryDirectory;
  private static Path bucketPath;

  // Nessie

  @NessieBaseUri
  private static URI nessieUri;

  private static NessieApiV2 nessieApi;
  private static DataplanePlugin dataplanePlugin;

  private static Catalog catalog;
  private static NamespaceService namespaceService;
  private static NessieCatalog nessieIcebergCatalog;

  @BeforeAll
  static void setUp() throws Exception {
    setUpS3Mock();
    setUpNessie();
    setUpSabotNodeRule();
    setUpDataplanePlugin();
    setUpNessieIcebergCatalog();
    enableUseSyntax();
    setSystemOption(ENABLE_ICEBERG_TIME_TRAVEL, "true");
    setSystemOption(REFLECTION_VERSIONED_SOURCE_ENABLED, "true");
    setSystemOption(ENABLE_ICEBERG_SORT_ORDER, "true");
    setSystemOption(ENABLE_ICEBERG_TABLE_PROPERTIES, "true");
  }

  @AfterAll
  static void tearDown() throws Exception {
    AutoCloseables.close(nessieApi);
    if (s3Mock != null) {
      s3Mock.shutdown();
      s3Mock = null;
    }
    resetSystemOption(ENABLE_ICEBERG_TIME_TRAVEL.getOptionName());
    resetSystemOption(REFLECTION_VERSIONED_SOURCE_ENABLED.getOptionName());
    resetSystemOption(ENABLE_ICEBERG_SORT_ORDER.getOptionName());
    resetSystemOption(ENABLE_ICEBERG_TABLE_PROPERTIES.getOptionName());
  }

  @BeforeEach
  // Since we can't set to an empty context, setting it to some known but *wrong* context on purpose.
  void before() throws Exception {
    // Note: dfs_hadoop is immutable.
    BaseTestQuery.test(String.format("USE %s", DATAPLANE_PLUGIN_NAME));
    Branch defaultBranch = getNessieClient().getDefaultBranch();
    getNessieClient().getAllReferences().stream()
      .forEach(
        ref -> {
          try {
            if (ref instanceof Branch && !ref.getName().equals(defaultBranch.getName())) {
              getNessieClient().deleteBranch().branch((Branch) ref).delete();
            } else if (ref instanceof Tag) {
              getNessieClient().deleteTag().tag((Tag) ref).delete();
            }
          } catch (NessieConflictException | NessieNotFoundException e) {
            throw new RuntimeException(e);
          }
        });

    getNessieClient().assignBranch().branch(defaultBranch).assignTo(Detached.of(NO_ANCESTOR)).assign();
  }

  protected static void setUpS3Mock() throws IOException {
    Preconditions.checkState(s3Mock == null);

    // We use S3Mock's in-memory backend implementation to avoid incompatibility issues between Hadoop's S3's implementation
    // and S3Mock's filesystem backend. When doing file deletions, Hadoop executes a "maybeCreateFakeParentDirectory"
    // operation that tries to write a 0 byte object to S3. S3Mock's filesystem backend throws an AmazonS3Exception
    // with a "Is a directory" message. The in-memory backend does not have the same issue.
    // We encountered this problem (in tests only, not AWS S3) when cleaning up Iceberg metadata files after a failed Nessie commit.
    s3Mock = new S3Mock.Builder()
      .withPort(0)
      .withInMemoryBackend()
      .build();

    s3Port = s3Mock.start().localAddress().getPort();

    EndpointConfiguration endpoint = new EndpointConfiguration(String.format("http://localhost:%d", s3Port), Region.US_EAST_1.toString());

    s3Client = AmazonS3ClientBuilder
      .standard()
      .withPathStyleAccessEnabled(true)
      .withEndpointConfiguration(endpoint)
      .withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials()))
      .build();

    s3Client.createBucket(BUCKET_NAME);
    s3Client.createBucket(ALTERNATIVE_BUCKET_NAME);
  }

  protected static void setUpNessie() {
    nessieApi = HttpClientBuilder.builder().withUri(createNessieURIString()).build(NessieApiV2.class);
  }

  private static void setUpSabotNodeRule() throws Exception {
    SABOT_NODE_RULE.register(new AbstractModule() {
      @Override
      protected void configure() {
        bind(SysFlightPluginConfigProvider.class).toInstance(new SysFlightPluginConfigProvider());

        // For System Table set
        final ConduitServiceRegistry conduitServiceRegistry = new ConduitServiceRegistryImpl();

        final boolean isMaster = true;
        NessieService nessieService = new NessieService(
          getProvider(KVStoreProvider.class),
          getProvider(OptionManager.class),
          getProvider(SchedulerService.class),
          false,
          () -> isMaster
        );
        nessieService.getGrpcServices().forEach(conduitServiceRegistry::registerService);

        final DatasetCatalogServiceImpl datasetCatalogServiceImpl = new DatasetCatalogServiceImpl(
          getProvider(CatalogService.class),
          getProvider(NamespaceService.Factory.class)
        );
        bind(DatasetCatalogServiceImpl.class).toInstance(datasetCatalogServiceImpl);
        conduitServiceRegistry.registerService(datasetCatalogServiceImpl);

        conduitServiceRegistry.registerService(new InformationSchemaServiceImpl(getProvider(CatalogService.class),
          () -> new ContextMigratingExecutorService.ContextMigratingCloseableExecutorService<>(new CloseableThreadPool("DataplaneEnterpriseTestSetup-"))));

        bind(ConduitServiceRegistry.class).toInstance(conduitServiceRegistry);
        // End System Table set

        bind(ModifiableSchedulerService.class).toInstance(mock(ModifiableSchedulerService.class));
        bind(CatalogService.class).toInstance(
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
            DremioConfig.create(),
            EnumSet.allOf(ClusterCoordinator.Role.class),
            getProvider(ModifiableSchedulerService.class),
            getProvider(VersionedDatasetAdapterFactory.class))
        );
      }
    });
    setupDefaultTestCluster();
  }

  protected static NessiePluginConfig prepareConnectionConf(String bucket) {
    NessiePluginConfig nessiePluginConfig = new NessiePluginConfig();
    nessiePluginConfig.nessieEndpoint = createNessieURIString();
    nessiePluginConfig.nessieAuthType = NessieAuthType.NONE;
    nessiePluginConfig.secure = false;
    nessiePluginConfig.awsAccessKey = "foo"; // Unused, just needs to be set
    nessiePluginConfig.awsAccessSecret = "bar"; // Unused, just needs to be set
    nessiePluginConfig.awsRootPath = bucket;

    // S3Mock settings
    nessiePluginConfig.propertyList = Arrays.asList(
      new Property("fs.s3a.endpoint", "localhost:" + s3Port),
      new Property("fs.s3a.path.style.access", "true"),
      new Property("fs.s3a.connection.ssl.enabled", "false"),
      new Property("fs.s3.impl", TestExtendedS3AFilesystem.class.getName()),
      new Property("fs.s3a.impl", TestExtendedS3AFilesystem.class.getName()),
      new Property(S3FileSystem.COMPATIBILITY_MODE, "true"));

    return nessiePluginConfig;
  }

  protected static void setUpDataplanePlugin() {
    getSabotContext().getOptionManager().setOption(OptionValue.createBoolean(SYSTEM, NESSIE_PLUGIN_ENABLED.getOptionName(), true));
    getSabotContext().getOptionManager().setOption(OptionValue.createBoolean(SYSTEM, VERSIONED_VIEW_ENABLED.getOptionName(), true));

    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();

    SourceConfig sourceConfig = new SourceConfig()
      .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
      .setName(DATAPLANE_PLUGIN_NAME)
      .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
    catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
    dataplanePlugin = catalogImpl.getSystemUserCatalog().getSource(DATAPLANE_PLUGIN_NAME);
    catalog = catalogImpl.getSystemUserCatalog();

    namespaceService = getSabotContext().getNamespaceService(SystemUser.SYSTEM_USERNAME);
  }

  public void setupForCreatingSources(List<String> sourceNames) {
    sourceNames.forEach(sourceName -> {
      CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
      SourceConfig sourceConfig = new SourceConfig()
        .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
        .setName(sourceName)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
      catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
    });
  }

  public void cleanupForDeletingSources(List<String> sourceNames) {
    sourceNames.forEach(sourceName -> {
      CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
      SourceConfig sourceConfig = new SourceConfig()
        .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
        .setName(sourceName)
        .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
      catalogImpl.getSystemUserCatalog().deleteSource(sourceConfig);
    });
  }

  /**
   * Defaulted to use DataplanePlugin Connection configuration if not passed
   *
   * @param sourceNamesWithConnectionConf
   */
  public static void setupForCreatingSources(Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf) {
    sourceNamesWithConnectionConf.forEach((sourceName, connectionConf) -> {
      CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
      if (catalogImpl.getAllVersionedPlugins().noneMatch(s -> s.getName().equals(sourceName))) {
        SourceConfig sourceConfig;
        if (connectionConf != null) {
          sourceConfig = new SourceConfig()
            .setConnectionConf(connectionConf)
            .setName(sourceName)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
        } else {
          sourceConfig = new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(sourceName)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
        }
        catalogImpl.getSystemUserCatalog().createSource(sourceConfig);
      }
    });
  }

  /**
   * Defaulted to use DataplanePlugin Connection configuration if not passed
   *
   * @param sourceNamesWithConnectionConf
   */
  public static void cleanupForDeletingSources(Map<String, AbstractConnectionConf> sourceNamesWithConnectionConf) {
    sourceNamesWithConnectionConf.forEach((sourceName, connectionConf) -> {
      CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
      if (catalogImpl.getAllVersionedPlugins().anyMatch(s -> s.getName().equals(sourceName))) {
        SourceConfig sourceConfig;
        if (connectionConf != null) {
          sourceConfig = new SourceConfig()
            .setConnectionConf(connectionConf)
            .setName(sourceName)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
        } else {
          sourceConfig = new SourceConfig()
            .setConnectionConf(prepareConnectionConf(BUCKET_NAME))
            .setName(sourceName)
            .setMetadataPolicy(CatalogService.NEVER_REFRESH_POLICY);
        }
        catalogImpl.getSystemUserCatalog().deleteSource(sourceConfig);
      }
    });
  }

  protected static void setUpNessieIcebergCatalog() throws NessieNotFoundException {
    nessieIcebergCatalog = new NessieCatalog();
    Branch defaultRef = nessieApi.getDefaultBranch();

    NessieIcebergClient nessieIcebergClient = new NessieIcebergClient(nessieApi,
        defaultRef.getName(), defaultRef.getHash(), new HashMap<>());

    AwsCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(
        AwsBasicCredentials.create("foo", "bar"));

    URI endpointUri = URI.create("http://localhost:" + s3Port);
    S3FileIO s3FileIO = new S3FileIO(() -> S3Client.builder().endpointOverride(endpointUri)
        .region(Region.US_EAST_1).credentialsProvider(credentialsProvider).build());
    s3FileIO.initialize(Collections.emptyMap());

    nessieIcebergCatalog.initialize("test", nessieIcebergClient, s3FileIO,
        ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET_NAME));
  }

  public Table loadTable(String table) {
    return nessieIcebergCatalog.loadTable(TableIdentifier.parse(table));
  }

  public Table createTable(String table, Map<String, String> properties) {
    Schema icebergTableSchema = new Schema(ImmutableList.of(Types.NestedField.required(0, "c1", new Types.IntegerType())));
    TableIdentifier tableId = TableIdentifier.parse(table);
    String location = String.format("s3://%s/%s", BUCKET_NAME, table);
    return nessieIcebergCatalog
      .createTable(tableId, icebergTableSchema, PartitionSpec.unpartitioned(), location, properties);
  }

  public void runWithAlternateSourcePath(String sql) throws Exception {
    try (AutoCloseable resetPath = () -> changeSourceRootPath(BUCKET_NAME)) {
      changeSourceRootPath(ALTERNATIVE_BUCKET_NAME);
      runSQL(sql);
    }
  }

  private static void changeSourceRootPath(String bucket) throws NamespaceException {
    SourceConfig sourceConfig = namespaceService.getSource(new NamespaceKey(DATAPLANE_PLUGIN_NAME));
    sourceConfig.setConnectionConf(prepareConnectionConf(bucket));
    CatalogServiceImpl catalogImpl = (CatalogServiceImpl) getSabotContext().getCatalogService();
    catalogImpl.getSystemUserCatalog().updateSource(sourceConfig);

    // get ref to new DataplanePlugin (old one was closed during ManagedStoragePlugin.replacePlugin)
    dataplanePlugin = catalogImpl.getSystemUserCatalog().getSource(DATAPLANE_PLUGIN_NAME);
  }

  public static NessieApiV2 getNessieClient() {
    return nessieApi;
  }

  public DataplanePlugin getDataplanePlugin() {
    return dataplanePlugin;
  }

  public Catalog getContextualizedCatalog(String pluginName, VersionContext versionContext) {
    Catalog resetCatalog = getCatalog().resolveCatalogResetContext(DATAPLANE_PLUGIN_NAME, versionContext);
    Catalog newCatalog = resetCatalog.resolveCatalog(ImmutableMap.of(DATAPLANE_PLUGIN_NAME, versionContext));
    return newCatalog;
  }

  public Catalog getCatalog() {
    return catalog;
  }

  protected static AmazonS3 getS3Client() {
    return s3Client;
  }

  public void assertAllFilesAreInBaseBucket(List<String> tablePath) throws Exception {
    List<List<String>> fileLocations = runSqlWithResults(selectFileLocationsQuery(tablePath));
    fileLocations.stream().flatMap(List::stream).forEach(loc -> assertTrue(loc.startsWith(S3_PREFIX + BUCKET_NAME)));
  }

  public void assertAllFilesInAlternativeBucket(List<String> tablePath) throws Exception {
    List<List<String>> fileLocations = runSqlWithResults(selectFileLocationsQuery(tablePath));
    fileLocations.stream().flatMap(List::stream).forEach(loc -> assertTrue(loc.startsWith(S3_PREFIX + ALTERNATIVE_BUCKET_NAME)));
  }

  protected static File createTempLocation() {
    String locationName = RandomStringUtils.randomAlphanumeric(8);
    File location = new File(getDfsTestTmpSchemaLocation(), locationName);
    location.mkdirs();
    return location;
  }

  protected static void createFolders(List<String> tablePath, VersionContext versionContext) throws Exception {
    //Iterate to get the parent folders where the table should be created (till tableName). Last element is tableName
    StringBuilder folderName = new StringBuilder();
    folderName.append(DATAPLANE_PLUGIN_NAME);
    for (int i = 0; i < tablePath.size() - 1; i++) {
      folderName.append(".").append(tablePath.get(i));
      runSQL(createFolderAtQueryWithIfNotExists(Collections.singletonList(folderName.toString()), versionContext));
    }
  }

  /* Helper function used to set up the NessieURI as a string. */
  protected static String createNessieURIString() {
    return nessieUri.toString()+"v2";
  }

  public void enableVacuumCatalogFF() {
    setSystemOption(ENABLE_ICEBERG_VACUUM_CATALOG, "true");
  }

  public void resetVacuumCatalogFF() {
    resetSystemOption(ENABLE_ICEBERG_VACUUM_CATALOG.getOptionName());
  }

  public void setNessieGCDefaultCutOffPolicy(String value) {
    ImmutableGarbageCollectorConfig gcConfig = ImmutableGarbageCollectorConfig.builder().defaultCutoffPolicy(value).build();
    try {
      getNessieClient().updateRepositoryConfig().repositoryConfig(gcConfig).update();
    } catch (NessieConflictException e) {
      throw new RuntimeException("Failed to update Nessie configs", e);
    }
  }

  public void setSliceTarget(String target) {
    setSystemOption(ExecConstants.SLICE_TARGET_OPTION, target);
  }

  public void resetSliceTarget() {
    resetSystemOption(SLICE_TARGET_OPTION.getOptionName());
  }

  public void setTargetBatchSize(String batchSize) {
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN, "1");
    setSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX, batchSize);
  }

  public void resetTargetBatchSize() {
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MIN.getOptionName());
    resetSessionOption(ExecConstants.TARGET_BATCH_RECORDS_MAX.getOptionName());
  }
}
