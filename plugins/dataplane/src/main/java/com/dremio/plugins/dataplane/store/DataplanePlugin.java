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
package com.dremio.plugins.dataplane.store;

import static com.dremio.plugins.dataplane.NessiePluginConfigConstants.MINIMUM_NESSIE_SPECIFICATION_VERSION;
import static com.dremio.plugins.dataplane.store.InformationSchemaCelFilter.getInformationSchemaFilter;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Provider;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.view.ViewDefinition;
import org.apache.iceberg.view.ViewVersionMetadata;
import org.apache.iceberg.view.ViewVersionMetadataParser;
import org.projectnessie.client.api.NessieApi;

import com.dremio.common.AutoCloseables;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.DataplaneTableInfo;
import com.dremio.exec.catalog.DataplaneViewInfo;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionContext;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.physical.config.TableFunctionConfig;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.BlockBasedSplitGenerator;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.ConnectionRefusedException;
import com.dremio.exec.store.HttpClientRequestException;
import com.dremio.exec.store.InvalidSpecificationVersionException;
import com.dremio.exec.store.InvalidURLException;
import com.dremio.exec.store.NessieApiProvider;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.SemanticVersionParserException;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.UnAuthenticatedException;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.dfs.AddPrimaryKey;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.DropPrimaryKey;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemRulesFactory;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.exec.store.iceberg.TimeTravelProcessors;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedViews;
import com.dremio.exec.store.metadatarefresh.footerread.FooterReadTableFunction;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetScanTableFunction;
import com.dremio.exec.store.parquet.ParquetSplitCreator;
import com.dremio.exec.store.parquet.ScanTableFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.ExternalNamespaceEntry.Type;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClient.NestingMode;
import com.dremio.plugins.NessieClientTableMetadata;
import com.dremio.plugins.s3.store.S3FileSystem;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.plugins.util.awsauth.AWSCredentialsConfigurator;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.fragment.FragmentExecutionContext;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.TableType;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.telemetry.api.metrics.MetricsInstrumenter;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;

import io.opentelemetry.instrumentation.annotations.WithSpan;

/**
 * Plugin to represent Dremio Dataplane (DDP) Catalog in Dremio Query Engine (DQE).
 */
public class DataplanePlugin extends FileSystemPlugin<AbstractDataplanePluginConfig>
  implements VersionedPlugin, MutablePlugin, NessieApiProvider {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DataplanePlugin.class);
  private static final MetricsInstrumenter metrics = new MetricsInstrumenter(DataplanePlugin.class);
  private static final String DEFAULT_CATALOG_NAME = "DREMIO";
  private final AbstractDataplanePluginConfig pluginConfig;
  private final SabotContext context;
  private final String name;

  private final NessieClient nessieClient;
  private final Configuration fileSystemConfig;
  private final AWSCredentialsConfigurator awsCredentialConfigurator;

  /**
   * The cached DremioFileIO instance for the plugin.  This is created on-demand - consumers should access this only
   * via the getFileIO() method which handles the creation.
   */
  private FileIO fileIO;

  private static final Joiner DOT_JOINER = Joiner.on('.');

  private final LoadingCache<ImmutablePair<String, EntityPath>, Table> tableLoadingCache = Caffeine
    .newBuilder()
    .maximumSize(1000) // items
    .softValues()
    .expireAfterAccess(1, TimeUnit.HOURS)
    .build(new TableCacheLoader());

  private final LoadingCache<String, ViewVersionMetadata> viewLoadingCache = Caffeine
    .newBuilder()
    .maximumSize(1000) // items
    .softValues()
    .expireAfterAccess(1, TimeUnit.HOURS)
    .build(new ViewCacheLoader());

  public DataplanePlugin(AbstractDataplanePluginConfig pluginConfig,
                         SabotContext context,
                         String name,
                         Provider<StoragePluginId> idProvider,
                         AWSCredentialsConfigurator awsCredentialsConfigurator,
                         NessieClient nessieClient) {
    super(pluginConfig, context, name, idProvider);
    this.pluginConfig = pluginConfig;
    this.context = context;
    this.name = name;
    this.awsCredentialConfigurator = awsCredentialsConfigurator;

    this.nessieClient = nessieClient;

    this.fileSystemConfig = initializeFileSystemConfig();
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    ResolvedVersionContext resolvedVersionContext = nessieClient.resolveVersionContext(versionContext);
    logger.debug("VersionContext '{}' resolved to '{}'", versionContext, resolvedVersionContext);
    return resolvedVersionContext;
  }

  @Override
  @WithSpan
  public boolean commitExists(String commitHash) {
    return metrics.log("commitExists", () -> nessieClient.commitExists(commitHash));
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listBranches() {
    return metrics.log("listBranches", nessieClient::listBranches);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listTags() {
    return metrics.log("listTags", nessieClient::listTags);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listReferences() {
    return metrics.log("listReferences", nessieClient::listReferences);
  }

  @Override
  @WithSpan
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    return metrics.log("listChanges", () -> nessieClient.listChanges(version));
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listEntries(List<String> catalogPath, VersionContext version) {
    return metrics.log("listEntries", () -> {
      ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
      return nessieClient.listEntries(catalogPath, resolvedVersion,
        NestingMode.SAME_DEPTH_ONLY, null, null);
    });
  }

  @VisibleForTesting
  @Override
  public Stream<ExternalNamespaceEntry> listEntriesIncludeNested(List<String> catalogPath, VersionContext version) {
    return metrics.log("listEntriesIncludeNested",
      () -> listEntriesIncludeNestedHelper(catalogPath, version, null));
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listTablesIncludeNested(List<String> catalogPath, VersionContext version)
  {
    return metrics.log("listTablesIncludeNested",
      () -> listEntriesIncludeNestedHelper(catalogPath, version, EnumSet.of(Type.ICEBERG_TABLE)));
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listViewsIncludeNested(List<String> catalogPath, VersionContext version)
  {
    return metrics.log("listViewsIncludeNested",
      () -> listEntriesIncludeNestedHelper(catalogPath, version, EnumSet.of(Type.ICEBERG_VIEW)));
  }

  private Stream<ExternalNamespaceEntry> listEntriesIncludeNestedHelper(
    List<String> catalogPath,
    VersionContext version,
    @Nullable Set<ExternalNamespaceEntry.Type> contentFilter
  ) {
    ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
    return nessieClient.listEntries(catalogPath, resolvedVersion,
      NestingMode.INCLUDE_NESTED, contentFilter, null);
  }

  @Override
  @WithSpan
  public EntityType getType(List<String> tableKey, ResolvedVersionContext version) {
    return metrics.log("getType()", () -> nessieClient.getVersionedEntityType(tableKey, version));
  }

  @Override
  @WithSpan
  public void createNamespace(NamespaceKey namespaceKey, VersionContext version) {
    logger.debug("Creating namespace '{}' from '{}'", namespaceKey, version);
    metrics.log("createNamespace", () -> nessieClient.createNamespace(schemaComponentsWithoutPluginName(namespaceKey), version));
  }

  @Override
  @WithSpan
  public void deleteFolder(NamespaceKey namespaceKey, VersionContext version) {
    logger.debug("Deleting Folder '{}' from '{}'", namespaceKey, version);
    metrics.log("deleteFolder", () -> nessieClient.deleteNamespace(schemaComponentsWithoutPluginName(namespaceKey), version));
  }

  @Override
  @WithSpan
  public void createBranch(String branchName, VersionContext sourceVersion) {
    logger.debug("Creating branch '{}' from '{}'", branchName, sourceVersion);
    metrics.log("createBranch", () -> nessieClient.createBranch(branchName, sourceVersion));
  }

  @Override
  @WithSpan
  public void createTag(String tagName, VersionContext sourceVersion) {
    logger.debug("Creating tag '{}' from '{}'", tagName, sourceVersion);
    metrics.log("createTag", () -> nessieClient.createTag(tagName, sourceVersion));
  }

  @Override
  @WithSpan
  public void dropBranch(String branchName, String branchHash) {
    logger.debug("Drop branch '{}' at '{}'", branchName, branchHash);
    metrics.log("dropBranch", () -> nessieClient.dropBranch(branchName, branchHash));
  }

  @Override
  @WithSpan
  public void dropTag(String tagName, String tagHash) {
    logger.debug("Dropping tag '{}' at '{}'", tagName, tagHash);
    metrics.log("dropTag", () -> nessieClient.dropTag(tagName, tagHash));
  }

  @Override
  @WithSpan
  public void mergeBranch(String sourceBranchName, String targetBranchName) {
    logger.debug("Merging branch '{}' into '{}'", sourceBranchName, targetBranchName);
    metrics.log("mergeBranch", () -> nessieClient.mergeBranch(sourceBranchName, targetBranchName));
  }

  @Override
  @WithSpan
  public void assignBranch(String branchName, VersionContext sourceVersion)
      throws ReferenceConflictException, ReferenceNotFoundException {
    logger.debug("Assign branch '{}' to {}", branchName, sourceVersion);
    metrics.log("assignBranch", () -> nessieClient.assignBranch(branchName, sourceVersion));
  }

  @Override
  @WithSpan
  public void assignTag(String tagName, VersionContext sourceVersion)
    throws ReferenceConflictException, ReferenceNotFoundException {
    logger.debug("Assign tag '{}' to {}", tagName, sourceVersion);
    metrics.log("assignTag", () -> nessieClient.assignTag(tagName, sourceVersion));
  }

  @Override
  @WithSpan
  public Optional<DatasetHandle> getDatasetHandle(
    EntityPath datasetPath,
    GetDatasetOption... options
  ) {

    return metrics.log("getDatasetHandle", () -> {
      try{
        return getDatasetHandleHelper(datasetPath, options);
      } catch (UncheckedExecutionException e) {
        throw failedToLoadIcebergTableException(e);
      }
    });
  }

  private Optional<DatasetHandle> getDatasetHandleHelper(EntityPath datasetPath, GetDatasetOption[] options) {
    final ResolvedVersionContext version = Preconditions.checkNotNull(
        VersionedDatasetAccessOptions
            .getVersionedDatasetAccessOptions(options)
            .getVersionContext());
    logger.debug("Getting dataset handle for '{}' at version {} ",
      datasetPath,
      version);

    List<String> versionedTableKey = datasetPath.getComponents().subList(1, datasetPath.size());
    EntityType entityType = getType(versionedTableKey, version);
    final String metadataLocation = nessieClient.getMetadataLocation(
      versionedTableKey,
      version,
      null);
    logger.debug("Retrieving Iceberg metadata from location '{}' ", metadataLocation);
    if (metadataLocation == null) {
      return Optional.empty();
    }

    final String contentId = nessieClient.getContentId(versionedTableKey, version, null) ;
    final String uniqueId = getUUIDFromMetadataLocation(metadataLocation);

    switch(entityType) {
      case ICEBERG_TABLE:
        final Table table = getIcebergTable(datasetPath, metadataLocation);
        logger.debug("Retrieved Iceberg table : name {} , location {}, schema {}, current snapshot {}, partition spec {} ",
          table.name(),
          table.location(),
          table.schema(),
          table.currentSnapshot(),
          table.spec());

        final TimeTravelOption travelOption = TimeTravelOption.getTimeTravelOption(options);
        final TimeTravelOption.TimeTravelRequest timeTravelRequest =
          travelOption != null ? travelOption.getTimeTravelRequest() : null;
        final TableSnapshotProvider tableSnapshotProvider =
          TimeTravelProcessors.getTableSnapshotProvider(datasetPath.getComponents(), timeTravelRequest);
        logger.debug("Time travel request {} ", timeTravelRequest);
        final TableSchemaProvider tableSchemaProvider =
                TimeTravelProcessors.getTableSchemaProvider(timeTravelRequest);
        return Optional.of(new TransientIcebergMetadataProvider(datasetPath,
          Suppliers.ofInstance(table),
          fileSystemConfig,
          tableSnapshotProvider,
          this,
          tableSchemaProvider,
          context.getOptionManager(),
          contentId,
          uniqueId));

      case ICEBERG_VIEW:
        final Optional<String> dialect = nessieClient.getViewDialect(versionedTableKey, version);
        if (!dialect.isPresent() || !dialect.get().equals(IcebergNessieVersionedViews.DIALECT)) {
          throw UserException.validationError()
              .message(
                  "Dialect is %s and %s is expected",
                  dialect.isPresent() ? dialect.get() : null, IcebergNessieVersionedViews.DIALECT)
              .build();
        }

        final ViewVersionMetadata viewVersionMetadata = readViewMetadata(metadataLocation);

        return Optional.of(ViewHandle
          .newBuilder()
          .datasetpath(datasetPath)
          .viewVersionMetadata(viewVersionMetadata)
          .id(contentId)
          .uniqueId(uniqueId)
          .build());
      default:
        return Optional.empty();
    }
  }

  private String getUUIDFromMetadataLocation(String metadataLocation){
    return metadataLocation.substring(metadataLocation.lastIndexOf("/")+1, metadataLocation.lastIndexOf(".metadata.json"));
  }

  private UserException failedToLoadIcebergTableException(Exception e) {
    return UserException.ioExceptionError(e)
      .message("Failed to load the Iceberg table. The underlying metadata and/or data files may not exist, " +
        "or you do not have permission to access them.")
      .buildSilently();
  }

  private Table getIcebergTable(EntityPath datasetPath, String metadataLocation) {
    return metrics.log("loadIcebergTable", () -> {
      LoadingCache<ImmutablePair<String, EntityPath>, Table> cache = getTableLoadingCache();
      return cache.get(ImmutablePair.of(metadataLocation, datasetPath));
    });
  }

  private LoadingCache<ImmutablePair<String, EntityPath>, Table> getTableLoadingCache() {
    return tableLoadingCache;
  }

  @Override
  @WithSpan
  public PartitionChunkListing listPartitionChunks(
    DatasetHandle datasetHandle,
    ListPartitionChunkOption... options
  ) {
    return metrics.log("listPartitionChunks", () -> {
      TransientIcebergMetadataProvider icebergMetadataProvider =
        datasetHandle.unwrap(TransientIcebergMetadataProvider.class);
      return icebergMetadataProvider.listPartitionChunks(options);
    });
  }

  @Override
  @WithSpan
  public DatasetMetadata getDatasetMetadata(
    DatasetHandle datasetHandle,
    PartitionChunkListing chunkListing,
    GetMetadataOption... options
  ) {
    return metrics.log("getDatasetMetadata", ()-> {
      TransientIcebergMetadataProvider icebergMetadataProvider =
        datasetHandle.unwrap(TransientIcebergMetadataProvider.class);
      return icebergMetadataProvider.getDatasetMetadata(options);
    });
  }

  @Override
  public boolean containerExists(EntityPath containerPath) {
    return false;
  }

  @Override
  public boolean hasAccessPermission(String user,
                                     NamespaceKey key,
                                     DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceState getState() {
    return this.pluginConfig.getState(nessieClient, name, context);
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE; // TODO(DX-43872) Are there any source capabilities we should add?
  }

  @Override
  public ViewTable getView(
    List<String> tableSchemaPath,
    SchemaConfig schemaConfig
  ) {
    throw new UnsupportedOperationException("Views aren't supported");
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return context.getConfig().getClass("dremio.plugins.dfs.rulesfactory", StoragePluginRulesFactory.class, FileSystemRulesFactory.class);
  }

  @Override
  public void start() throws IOException {
    this.pluginConfig.validatePluginEnabled(context);
    this.pluginConfig.validateNessieAuthSettings(name);
    this.pluginConfig.validateAWSRootPath(getRootLocation());
    try {
      this.pluginConfig.validateNessieSpecificationVersion(nessieClient, name);
    } catch (InvalidURLException e) {
      throw UserException.validationError(e).message("Unable to create source [%s], " +
          "Make sure that Nessie endpoint URL is valid.", name).buildSilently();
    } catch (InvalidSpecificationVersionException e) {
      throw UserException.validationError(e).message("Unable to create source [%s], Nessie Server should comply with Nessie specification version %s or later." +
        " Also make sure that Nessie endpoint URL is valid.", name, MINIMUM_NESSIE_SPECIFICATION_VERSION).buildSilently();
    } catch (SemanticVersionParserException e) {
      throw UserException.validationError(e).message("Unable to create source [%s], Cannot parse Nessie specification version." +
        " Nessie Server should comply with Nessie specification version %s or later.", name, MINIMUM_NESSIE_SPECIFICATION_VERSION).buildSilently();
    }
    try {
      this.pluginConfig.validateConnectionToNessieRepository(nessieClient, name, context);
    } catch (NoDefaultBranchException e){
      throw UserException.resourceError().message("Unable to create source [%s], No default branch exists in Nessie Server", name).buildSilently();
    } catch (UnAuthenticatedException ex) {
      throw UserException.resourceError().message("Unable to create source [%s], Unable to authenticate to the Nessie server. " +
        "Make sure that the token is valid and not expired.", name).buildSilently();
    } catch (ConnectionRefusedException ex) {
      throw UserException.resourceError().message("Unable to create source [%s], Connection refused while " +
        "connecting to the Nessie Server.", name).buildSilently();
    } catch (HttpClientRequestException ex) {
      throw UserException.resourceError().message("Unable to create source [%s], Failed to get the default branch from" +
        " the Nessie server.", name).buildSilently();
    }

    super.start();
  }

  @Override
  @WithSpan
  public void createEmptyTable(NamespaceKey tableSchemaPath,
                               SchemaConfig schemaConfig,
                               BatchSchema batchSchema,
                               WriterOptions writerOptions) {
    metrics.log("createEmptyTable",
      () -> createEmptyTableHelper(tableSchemaPath, schemaConfig, batchSchema, writerOptions));
  }

  private void createEmptyTableHelper(NamespaceKey tableSchemaPath,
                                      SchemaConfig schemaConfig,
                                      BatchSchema batchSchema,
                                      WriterOptions writerOptions) {
    final ResolvedVersionContext version = Preconditions.checkNotNull(writerOptions.getVersion());
    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);

    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null, // Used to create DremioInputFile (valid only for insert/ctas)
      version,
      this,
      schemaConfig.getUserName());

    logger.debug("Creating empty table: '{}' with version '{}'", tableSchemaComponentsWithoutPluginName, version);
    try {
      PartitionSpec partitionSpec = Optional.ofNullable(writerOptions.getTableFormatOptions().getIcebergSpecificOptions()
        .getIcebergTableProps()).map(props -> props.getDeserializedPartitionSpec()).orElse(null);
      icebergModel
        .getCreateTableCommitter(
          String.join(".", tableSchemaComponentsWithoutPluginName),
          icebergModel.getTableIdentifier(getRootLocation()),
          batchSchema,
          writerOptions.getPartitionColumns(),
          null,
          partitionSpec
          )
        .commit();
    } catch (UncheckedIOException e){
      if(e.getCause() instanceof ContainerAccessDeniedException){
        throw UserException.permissionError(e.getCause()).
          message("Access denied while creating table. %s",
            e.getMessage()
          )
          .buildSilently();
      }
      throw e;
    }
  }

  @Override
  @WithSpan
  public CreateTableEntry createNewTable(NamespaceKey tableSchemaPath,
                                         SchemaConfig schemaConfig,
                                         IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions,
                                         Map<String, Object> storageOptions,
                                         boolean isResultsTable) {
    return metrics.log("createNewTable",
      () -> createNewTableHelper(tableSchemaPath, schemaConfig, icebergTableProps, writerOptions));
  }

  private CreateTableEntry createNewTableHelper(NamespaceKey tableSchemaPath,
                                         SchemaConfig schemaConfig,
                                         IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions) {
    Preconditions.checkNotNull(icebergTableProps);
    Preconditions.checkNotNull(icebergTableProps.getVersion());
    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);

    final String tableName = String.join(".", tableSchemaComponentsWithoutPluginName);

    //Iceberg tables have parquet as the underlying file format
    final FormatPlugin formatPlugin = this.getFormatPlugin("parquet"); //This call uses FSPlugin formatCreator

    final String userName = schemaConfig.getUserName();

    Path path = resolveTableNameToValidPath(tableSchemaPath.toString(), writerOptions.getVersion());
    icebergTableProps = new IcebergTableProps(icebergTableProps);
    icebergTableProps.setTableLocation(path.toString());
    icebergTableProps.setTableName(tableName);
    Preconditions.checkState(icebergTableProps.getUuid() != null &&
      !icebergTableProps.getUuid().isEmpty(), "Unexpected state. UUID must be set");
    path = path.resolve(icebergTableProps.getUuid());
    logger.debug("Creating new table '{}' with options '{}' IcebergTableProps  '{}' ",
      tableSchemaPath,
      writerOptions,
      icebergTableProps
      );
    return new CreateParquetTableEntry(
      userName,
      this, // This requires FSPlugin features
      path.toString(),
      icebergTableProps,
      writerOptions,
      tableSchemaPath);
  }

  /**
   * Resolve given table path relative to source resolve it to a valid path in filesystem.
   * If the table exists, fetch the path from the versioned store. If not, resolve under base location.
   * If the resolved path refers to an entity not under the base of the source then a permission error is thrown.
   */
  private Path resolveTableNameToValidPath(String tablePathWithPlugin, ResolvedVersionContext versionContext) {
    List<String> tablePath = schemaComponentsWithoutPluginName(
      new NamespaceKey(PathUtils.parseFullPath(tablePathWithPlugin)));
    final String metadataLocation = nessieClient.getMetadataLocation(tablePath, versionContext, null);
    logger.info("Retrieving Iceberg metadata from location '{}' ", metadataLocation);

    if (metadataLocation == null) {
      // Table does not exist, resolve new path under the aws root folder location
      // location where the iceberg table folder will be created
      // Format : "<plugin.s3RootPath>"/"<folder1>/<folder2>/<tableName>"
      Path basePath = pluginConfig.getPath();
      String relativePathClean = PathUtils.removeLeadingSlash(String.join("/", tablePath));
      Path combined = basePath.resolve(relativePathClean);
      PathUtils.verifyNoAccessOutsideBase(basePath, combined);
      return combined;
    }

    final Table icebergTable = getIcebergTable(new EntityPath(tablePath), metadataLocation);
    return Path.of(removeUriScheme(icebergTable.location()));
  }

  private String removeUriScheme(String uri) {
    if (StringUtils.isBlank(uri)) {
      return uri;
    }

    int urlSchemeIndex = uri.indexOf("://");
    if (urlSchemeIndex > 0) {
      uri = uri.substring(urlSchemeIndex + 2);
    }
    return uri;
  }

  @Override
  public Writer getWriter(PhysicalOperator child,
                          String location,
                          WriterOptions options,
                          OpProps props) {
    throw new UnsupportedOperationException();
  }

  @Override
  @WithSpan
  public void dropTable(NamespaceKey tableSchemaPath,
                        SchemaConfig schemaConfig,
                        TableMutationOptions tableMutationOptions) {
    metrics.log("dropTable",
      () -> dropTableHelper(tableSchemaPath, schemaConfig, tableMutationOptions));
  }

  private void dropTableHelper(NamespaceKey tableSchemaPath,
                        SchemaConfig schemaConfig,
                        TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableKeyWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    //Check if the entity is a table.
    VersionedPlugin.EntityType entityType = nessieClient.getVersionedEntityType(tableKeyWithoutPluginName, version);
    if (entityType == EntityType.UNKNOWN ) {
      throw UserException.validationError()
        .message("%s does not exist ", tableKeyWithoutPluginName)
        .buildSilently();
    } else if (entityType != EntityType.ICEBERG_TABLE) {
      throw UserException.validationError()
        .message("%s is not a TABLE ", tableKeyWithoutPluginName)
        .buildSilently();
    }
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableKeyWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());

    logger.debug("Dropping table '{}' at version '{}'", tableKeyWithoutPluginName, version);
    icebergModel.deleteTable(icebergModel.getTableIdentifier(pluginConfig.awsRootPath));
  }

  @Override
  @WithSpan
  public void alterTable(NamespaceKey tableSchemaPath, DatasetConfig datasetConfig, AlterTableOption alterTableOption,
                          SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    metrics.log("updateTable",
      () -> alterTableHelper(tableSchemaPath, schemaConfig, alterTableOption, tableMutationOptions));
  }

  private void alterTableHelper(NamespaceKey tableSchemaPath,
                               SchemaConfig schemaConfig,
                               AlterTableOption alterTableOption,
                               TableMutationOptions tableMutationOptions) {
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
            getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    logger.debug("Altering table partition spec for table {} at version {} with options {}",
      tableSchemaComponentsWithoutPluginName,
      version,
      alterTableOption);
    icebergModel.alterTable(icebergModel.getTableIdentifier(getRootLocation()), alterTableOption);
  }

  @Override
  @WithSpan
  public void truncateTable(NamespaceKey tableSchemaPath,
                            SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    metrics.log("truncateTable",
      () -> truncateTableHelper(tableSchemaPath, schemaConfig, tableMutationOptions));
  }

  private void truncateTableHelper(NamespaceKey tableSchemaPath,
                     SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);

    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    logger.debug("Truncating table '{}' at version '{}'", tableSchemaComponentsWithoutPluginName, version);
    IcebergTableIdentifier icebergTableIdentifier = icebergModel.getTableIdentifier(getRootLocation());
    icebergModel.truncateTable(icebergTableIdentifier);
  }

  @Override
  @WithSpan
  public void rollbackTable(NamespaceKey tableSchemaPath,
                            DatasetConfig datasetConfig,
                            SchemaConfig schemaConfig,
                            RollbackOption rollbackOption,
                            TableMutationOptions tableMutationOptions) {

    metrics.log("rollbackTable",
      () -> rollbackTableHelper(tableSchemaPath, schemaConfig, rollbackOption, tableMutationOptions));
  }

  private void rollbackTableHelper(NamespaceKey tableSchemaPath,
                                SchemaConfig schemaConfig,
                                RollbackOption rollbackOption,
                                TableMutationOptions tableMutationOptions) {
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    logger.debug("Rollback table {} at version {} with options {}",
      tableSchemaComponentsWithoutPluginName,
      version,
      rollbackOption);
    icebergModel.rollbackTable(icebergModel.getTableIdentifier(getRootLocation()), rollbackOption);
  }

  @Override
  @WithSpan
  public boolean createOrUpdateView(NamespaceKey tableSchemaPath,
                                    SchemaConfig schemaConfig,
                                    View view,
                                    ViewOptions viewOptions) {
    return metrics.log("createOrUpdateView",
      () -> createOrUpdateViewHelper(tableSchemaPath, schemaConfig, view, viewOptions));
  }

  private boolean createOrUpdateViewHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      View view,
      ViewOptions viewOptions) {
    if (!viewOptions.getVersion().isBranch()) {
      throw UserException.validationError()
          .message("Cannot update a view on a tag or bareCommit")
          .buildSilently();
    }

    final ResolvedVersionContext version = Objects.requireNonNull(viewOptions.getVersion());
    final SchemaConverter converter = SchemaConverter.getBuilder().build();
    final List<String> viewKey = schemaComponentsWithoutPluginName(tableSchemaPath);
    final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(getRootLocation());

    try {
      final String metadata =
          IcebergUtils.getValidIcebergPath(
              path,
              fileSystemConfig,
              getSystemUserFS().getScheme());
      final IcebergNessieVersionedViews versionedViews =
          new IcebergNessieVersionedViews(metadata, nessieClient, fileSystemConfig, this, schemaConfig.getUserName());
      final ViewDefinition viewDefinition =
          viewOptions.isViewAlter()
              ? versionedViews.loadDefinition(viewKey, version)
              : ViewDefinition.of(
                  view.getSql(),
                  converter.toIcebergSchema(viewOptions.getBatchSchema()),
                  tableSchemaPath.getRoot(),
                  view.getWorkspaceSchemaPath());

      logger.debug(
          "{}: '{}' at source path '{}' with version '{}'",
          viewOptions.getActionType().name(),
          tableSchemaPath.getName(),
          tableSchemaPath,
          version);

      if (viewOptions.isViewCreate()) {
        versionedViews.create(viewKey, viewDefinition, Collections.emptyMap(), version);
        return true;
      }

      final String metadataLocation =
          Objects.requireNonNull(nessieClient.getMetadataLocation(viewKey, version, null));
      final ViewVersionMetadata viewVersionMetadata = readViewMetadata(metadataLocation);
      final Map<String, String> currentProperties = viewVersionMetadata.properties();

      if (viewOptions.isViewUpdate()) {
        versionedViews.replace(viewKey, viewDefinition, currentProperties, version);
        return true;
      }

      final Map<String, String> properties = Objects.requireNonNull(viewOptions.getProperties());
      final boolean needUpdate =
          properties.entrySet().stream()
              .anyMatch(entry -> !entry.getValue().equals(currentProperties.get(entry.getKey())));

      if (!needUpdate) {
        logger.debug("No property need to be updated");
        return false;
      }

      versionedViews.replace(viewKey, viewDefinition, properties, version);

      return true;
    } catch (UserException e) {
      throw e;
    } catch (Exception ex) {
      logger.debug("Exception while operating on the view", ex);
    }

    return false;
  }

  @Override
  @WithSpan
  public void dropView(NamespaceKey tableSchemaPath,
                       ViewOptions viewOptions, SchemaConfig schemaConfig) {
    metrics.log("dropView", () -> dropViewHelper(tableSchemaPath, schemaConfig, viewOptions));
  }

  private void dropViewHelper(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, ViewOptions viewOptions) {
    String location = getRootLocation();
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(location);
    String metadata = IcebergUtils.getValidIcebergPath(path,
      fileSystemConfig,
      getSystemUserFS().getScheme());
    IcebergNessieVersionedViews versionedViews =
      new IcebergNessieVersionedViews(metadata, nessieClient, fileSystemConfig, this, schemaConfig.getUserName());
    List<String> viewKey = schemaComponentsWithoutPluginName(tableSchemaPath);
    ResolvedVersionContext version = viewOptions.getVersion();
    logger.debug("Dropping view '{}' at version '{}'", viewKey, version);
    versionedViews.drop(viewKey, version);
  }

  @Override
  @WithSpan
  public void addColumns(NamespaceKey tableSchemaPath,
                         DatasetConfig datasetConfig,
                                  SchemaConfig schemaConfig,
                                  List<Field> columnsToAdd,
                                  TableMutationOptions tableMutationOptions) {
    metrics.log("addColumns",
      () -> addColumnsHelper(tableSchemaPath, schemaConfig, columnsToAdd, tableMutationOptions));
  }

  private void addColumnsHelper(NamespaceKey tableSchemaPath,
                                SchemaConfig schemaConfig,
                                List<Field> columnsToAdd,
                                TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    SchemaConverter schemaConverter = SchemaConverter.getBuilder().build();
    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    IcebergTableIdentifier icebergTableIdentifier = icebergModel.getTableIdentifier(getRootLocation());
    List<Types.NestedField> icebergFields = schemaConverter.toIcebergFields(columnsToAdd);

    logger.debug("Adding columns '{}' to table '{}' at version '{}'",
      columnsToAdd, tableSchemaComponentsWithoutPluginName, version);
    icebergModel.addColumns(icebergTableIdentifier, icebergFields);
  }

  @Override
  @WithSpan
  public void dropColumn(NamespaceKey tableSchemaPath,
                         DatasetConfig datasetConfig,
                                  SchemaConfig schemaConfig,
                                  String columnToDrop,
                                  TableMutationOptions tableMutationOptions) {
    metrics.log("dropColumn",
      () -> dropColumnHelper(tableSchemaPath, schemaConfig, columnToDrop, tableMutationOptions));
  }

  private void dropColumnHelper(NamespaceKey tableSchemaPath,
                         SchemaConfig schemaConfig,
                         String columnToDrop,
                         TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    IcebergTableIdentifier icebergTableIdentifier = icebergModel.getTableIdentifier(getRootLocation());
    logger.debug("Dropping column '{}' for table '{}' at version '{}'",
      columnToDrop, tableSchemaComponentsWithoutPluginName, version);
    icebergModel.dropColumn(icebergTableIdentifier, columnToDrop);
  }

  @Override
  @WithSpan
  public void changeColumn(NamespaceKey tableSchemaPath,
                                    DatasetConfig datasetConfig,
                                    SchemaConfig schemaConfig,
                                    String columnToChange,
                                    Field fieldFromSqlColDeclaration,
                                    TableMutationOptions tableMutationOptions) {
    metrics.log("changeColumn",
      () -> changeColumnHelper(tableSchemaPath, schemaConfig, columnToChange,
        fieldFromSqlColDeclaration, tableMutationOptions));
  }

  @Override
  @WithSpan
  public void addPrimaryKey(NamespaceKey table,
                            DatasetConfig datasetConfig,
                            SchemaConfig schemaConfig,
                            List<Field> columns,
                            ResolvedVersionContext versionContext) {
    metrics.log("addPrimaryKey",
      () -> addPrimaryKeyHelper(table, datasetConfig, schemaConfig, columns, versionContext));
  }

  private void addPrimaryKeyHelper(NamespaceKey table,
                                   DatasetConfig datasetConfig,
                                   SchemaConfig schemaConfig,
                                   List<Field> columns,
                                   ResolvedVersionContext versionContext) {
    AddPrimaryKey op = new AddPrimaryKey(table, context, datasetConfig, schemaConfig,
      getIcebergModelHelper(
        table.getPathComponents().subList(1, table.size()), // The key to nessie should not contain the source name, stripping source name.
        versionContext,
        null,
        schemaConfig.getUserName()),
      validateAndGetPath(table, schemaConfig.getUserName()),
      this);
    op.performOperation(columns);
  }

  @Override
  @WithSpan
  public void dropPrimaryKey(NamespaceKey table,
                             DatasetConfig datasetConfig,
                             SchemaConfig schemaConfig,
                             ResolvedVersionContext versionContext) {
    metrics.log("dropPrimaryKey",
      () -> dropPrimaryKeyHelper(table, datasetConfig, schemaConfig, versionContext));
  }

  private void dropPrimaryKeyHelper(NamespaceKey table,
                                    DatasetConfig datasetConfig,
                                    SchemaConfig schemaConfig,
                                    ResolvedVersionContext versionContext) {
    DropPrimaryKey op = new DropPrimaryKey(table, context, datasetConfig, schemaConfig,
      getIcebergModelHelper(
        table.getPathComponents().subList(1, table.size()), // The key to nessie should not contain the source name, stripping source name.
        versionContext,
        null,
        schemaConfig.getUserName()),
      validateAndGetPath(table, schemaConfig.getUserName()),
      this);
    op.performOperation();
  }

  @Override
  @WithSpan
  public List<String> getPrimaryKey(NamespaceKey table,
                                    DatasetConfig datasetConfig,
                                    SchemaConfig schemaConfig,
                                    ResolvedVersionContext versionContext,
                                    boolean saveInKvStore) {
    return metrics.log("getPrimaryKey",
      () -> getPrimaryKeyHelper(table, datasetConfig, schemaConfig, versionContext, saveInKvStore));
  }

  private List<String> getPrimaryKeyHelper(NamespaceKey table,
                                           DatasetConfig datasetConfig,
                                           SchemaConfig schemaConfig,
                                           ResolvedVersionContext versionContext,
                                           boolean saveInKvStore) {
    if (datasetConfig.getPhysicalDataset() == null || // PK only supported for physical datasets
      datasetConfig.getPhysicalDataset().getIcebergMetadata() == null || // Physical dataset not Iceberg format
      !DatasetHelper.isIcebergDataset(datasetConfig)) { // Not native iceberg
      return null;
    }

    return IcebergUtils.validateAndGeneratePrimaryKey(this, context, table, datasetConfig, schemaConfig, versionContext, saveInKvStore);
  }

  @Override
  @WithSpan
  public List<String> getPrimaryKeyFromMetadata(NamespaceKey table,
                                                DatasetConfig datasetConfig,
                                                SchemaConfig schemaConfig,
                                                ResolvedVersionContext versionContext,
                                                boolean saveInKvStore) {
    // For versioned tables, we don't cache the PK in the KV store. Grab the iceberg table
    // from the table cache.
    List<String> versionedTableKey = table.getPathComponents().subList(1, table.getPathComponents().size());
    final String metadataLocation = nessieClient.getMetadataLocation(versionedTableKey, versionContext, null);
    logger.debug("Retrieving Iceberg metadata from location '{}' ", metadataLocation);
    if (metadataLocation == null) {
      return null;
    }

    final Table icebergTable = getIcebergTable(new EntityPath(table.getPathComponents()), metadataLocation);
    return IcebergUtils.getPrimaryKeyFromTableMetadata(icebergTable);
  }

  private void changeColumnHelper(NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      String columnToChange,
      Field fieldFromSqlColDeclaration,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel = new IcebergNessieVersionedModel(
      tableSchemaComponentsWithoutPluginName,
      fileSystemConfig,
      getSystemUserFS(),
      nessieClient,
      null,
      version,
      this,
      schemaConfig.getUserName());
    IcebergTableIdentifier icebergTableIdentifier = icebergModel.getTableIdentifier(getRootLocation());

    logger.debug("Changing column '{}' to '{}' in table '{}' at version '{}'",
      columnToChange, fieldFromSqlColDeclaration, tableSchemaComponentsWithoutPluginName, version);

    icebergModel.changeColumn(icebergTableIdentifier, columnToChange, fieldFromSqlColDeclaration);
  }

  @Override
  public boolean toggleSchemaLearning(NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning) {
    throw new UnsupportedOperationException();
  }

  private Configuration initializeFileSystemConfig() {
    final Configuration config = FileSystemPlugin.getNewFsConf();
    // We maintain a separate Configuration within DataplanePlugin to keep it isolated from the one
    // in FileSystemPlugin. We may migrate away from extending FileSystemPlugin in the future.
    updateConfiguration(config);
    return config;
  }

  private List<String> schemaComponentsWithoutPluginName(NamespaceKey tableSchemaPath) {
    Preconditions.checkArgument(tableSchemaPath.hasParent());
    Preconditions.checkArgument(name.equalsIgnoreCase(tableSchemaPath.getRoot()));
    return tableSchemaPath.getPathWithoutRoot();
  }

  // This method is used to createFS out of the DataplanePluginConfig with appropriate AWS credential providers.
  @Override
  protected FileSystem newFileSystem(String userName, OperatorContext operatorContext) throws IOException {
    updateConfiguration(getFsConf());
    return super.newFileSystem(userName, operatorContext);
  }

  private void updateConfiguration(Configuration configuration) {
    configuration.set("fs.dremioS3.impl", S3FileSystem.class.getName());

    configuration.set(org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY, "dremioS3:///");

    // Enabling the cache will fetch the incorrect S3 file system object from Hadoop File System class.
    configuration.set("fs.dremioS3.impl.disable.cache", "true");

    for (Property property : pluginConfig.getProperties()) {
      configuration.set(property.name, property.value);
    }

    List<Property> awsProviderProperties = new ArrayList<>();
    String awsProvider = awsCredentialConfigurator.configureCredentials(awsProviderProperties);
    Optional<Property> property = pluginConfig.encryptConnection();
    if (property.isPresent()) {
      awsProviderProperties.add(property.get());
    }
    configuration.set(AWS_CREDENTIALS_PROVIDER, awsProvider);
    awsProviderProperties.forEach(p -> configuration.set(p.name, p.value));
  }

  @Override
  public Configuration getFsConfCopy() {
    return new Configuration(fileSystemConfig);
  }

  @Override
  public FormatPlugin getFormatPlugin(FormatPluginConfig formatConfig) {
    if (formatConfig instanceof ParquetFormatConfig) { // ParquetWriter requires this.
      return super.getFormatPlugin(formatConfig);
    } else {
      return new IcebergFormatPlugin("iceberg", context, (IcebergFormatConfig) formatConfig, this);
    }
  }

  @Override
  public List<String> resolveTableNameToValidPath(List<String> tableSchemaPath) {
    List<String> fullPath = new ArrayList<>(PathUtils.toPathComponents(pluginConfig.awsRootPath));
    for (String pathComponent : tableSchemaPath.subList(1 /* need to skip the source name */, tableSchemaPath.size())) {
      fullPath.add(PathUtils.removeQuotes(pathComponent));
    }
    PathUtils.verifyNoAccessOutsideBase(PathUtils.toFSPath(pluginConfig.awsRootPath), PathUtils.toFSPath(fullPath));
    return fullPath;
  }

  @Override
  public boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey key, NamespaceService userNamespaceService) {
    return true;
  }

  @Override
  public BlockBasedSplitGenerator.SplitCreator createSplitCreator(
    OperatorContext context,
    byte[] extendedBytes,
    boolean isInternalIcebergTable
  ) {
    return new ParquetSplitCreator(context, true);
  }

  @Override
  public ScanTableFunction createScanTableFunction(
    FragmentExecutionContext fec,
    OperatorContext context,
    OpProps props,
    TableFunctionConfig functionConfig
  ) {
    return new ParquetScanTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public FooterReadTableFunction getFooterReaderTableFunction(
    FragmentExecutionContext fec,
    OperatorContext context,
    OpProps props,
    TableFunctionConfig functionConfig
  ) {
    return new FooterReadTableFunction(fec, context, props, functionConfig);
  }

  @Override
  public AbstractRefreshPlanBuilder createRefreshDatasetPlanBuilder(
    SqlHandlerConfig config,
    SqlRefreshDataset sqlRefreshDataset,
    UnlimitedSplitsMetadataProvider metadataProvider,
    boolean isFullRefresh
  ) {
    throw new UnsupportedOperationException("Metadata refresh is not supported");
  }

  @Override
  public IcebergModel getIcebergModel(IcebergTableProps icebergTableProps, String userName, OperatorContext operatorContext, FileSystem fileSystem) {
    List<String> tableKeyAsList = Arrays.asList(icebergTableProps.getTableName().split(Pattern.quote(".")));
    ResolvedVersionContext version = icebergTableProps.getVersion();
    return getIcebergModelHelper(tableKeyAsList, version, operatorContext, userName);
  }

  private IcebergModel getIcebergModelHelper(List<String> tableKeyAsList,
                                             ResolvedVersionContext version,
                                             OperatorContext operatorContext,
                                             String userName) {
    Preconditions.checkNotNull(tableKeyAsList);
    Preconditions.checkNotNull(version);
    Preconditions.checkNotNull(nessieClient);
    return new IcebergNessieVersionedModel(
      tableKeyAsList,
      getFsConfCopy(),
      getSystemUserFS(),
      nessieClient,
      operatorContext,
      version,
      this,
      userName);
  }

  @Override
  public String getRootLocation() {
    return pluginConfig.getPath().toString();
  }

  @Override
  public String getTableLocation(IcebergTableProps tableProps) {
    return getRootLocation();
  }

  public void commitTableGrpcOperation(List<String> catalogKey,
                                       String metadataLocation,
                                       NessieClientTableMetadata nessieClientTableMetadata,
                                       ResolvedVersionContext resolvedVersionContext,
                                       String baseContentId,
                                       String jobId,
                                       String userName) {
    nessieClient.commitTable(catalogKey, metadataLocation, nessieClientTableMetadata, resolvedVersionContext, baseContentId, jobId, userName);
  }

  public String getMetadataLocationGrpcOperation(List<String> catalogKey,
                                               ResolvedVersionContext resolvedVersionContext, String jobId) {
    return nessieClient.getMetadataLocation(catalogKey, resolvedVersionContext, jobId);
  }

  public String getContentGrpcOperation(List<String> catalogKey,
                                                 ResolvedVersionContext resolvedVersionContext, String jobId) {
    return nessieClient.getContentId(catalogKey, resolvedVersionContext, jobId);
  }

  @Override
  @WithSpan
  public String getContentId(List<String> key, ResolvedVersionContext resolvedVersionContext) {
    return nessieClient.getContentId(key, resolvedVersionContext, null);
  }

  @Override
  public NessieApi getNessieApi() {
    return nessieClient.getNessieApi();
  }

  private class TableCacheLoader implements CacheLoader<ImmutablePair<String, EntityPath>, Table> {
    @Override
    public Table load(ImmutablePair<String, EntityPath> pair) {
      return metrics.log("tableLoad", () -> loadTable(pair));
    }

    private Table loadTable(ImmutablePair<String, EntityPath> pair){
      String metadataLocation = pair.left;
      EntityPath datasetPath = pair.right;
      final TableOperations tableOperations = new StaticTableOperations(metadataLocation, getFileIO());
      if (tableOperations.current() == null) {
        logger.warn("Iceberg table content  at metadatalocation {} is null", metadataLocation);
        throw failedToLoadIcebergTableException(null);
      }
      final Table table = new BaseTable(tableOperations,
        String.join(".", datasetPath.getComponents().subList(1, datasetPath.size())));
      table.refresh();
      return table;
    }
  }

  private class ViewCacheLoader implements CacheLoader<String, ViewVersionMetadata> {
    @Override
    public ViewVersionMetadata load(String metadataLocation) {
      return metrics.log("viewLoad", () -> loadView(metadataLocation));
    }

    private ViewVersionMetadata loadView(String metadataLocation){
      return ViewVersionMetadataParser.read(getFileIO().newInputFile(metadataLocation));
    }
  }


  private Optional<String> determineSchemaId(ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    if (entry.getNamespace().isEmpty()) {
      return Optional.empty();
    }
    String schemaId = nessieClient.getContentId(entry.getNamespace(), resolvedVersionContext, null);
    if (schemaId == null) {
      logger.warn("Failed to retrieve schema information for entry: " + entry.getNameElements());
    }
    return Optional.ofNullable(schemaId);
  }

  private Optional<DataplaneViewInfo> dataplaneViewInfoRetriever(ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    try {
      List<String> catalogKey = entry.getNameElements();
      String metadataLocation = nessieClient.getMetadataLocation(catalogKey, resolvedVersionContext, null);
      EntityPath keyPath = toEntityPath(entry);
      Optional<String> schemaId = determineSchemaId(entry, resolvedVersionContext);
      ViewVersionMetadata viewVersionMetadata = readViewMetadata(metadataLocation);

      return Optional.of(new DataplaneViewInfo.newBuilder()
        .viewId(nessieClient.getContentId(catalogKey, resolvedVersionContext, null))
        .spaceId(this.getId().getConfig().getId().getId())
        .viewName(entry.getName())
        .schemaId(schemaId.orElse(""))
        .path(keyPath.toString())
        .tag(getUUIDFromMetadataLocation(metadataLocation))
        .createdAt(getViewCreatedAt(viewVersionMetadata))
        .sqlDefinition(getViewSqlDefinition(viewVersionMetadata))
        .sqlContext(getViewSqlContext(viewVersionMetadata))
        .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllViewInfo", e);
      // There is no way to propagate an error in SYS.VIEW queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  @Override
  @WithSpan
  public Stream<DataplaneViewInfo> getAllViewInfo() {
    ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
    return listAllEntries(resolvedVersionContext, EnumSet.of(Type.ICEBERG_VIEW))
      .map(entry -> dataplaneViewInfoRetriever(entry, resolvedVersionContext))
      .filter(Optional::isPresent)
      .map(Optional::get);
  }

  private ViewVersionMetadata readViewMetadata(String metadataLocation) {
    return viewLoadingCache.get(metadataLocation);
  }

  private static long getViewCreatedAt(ViewVersionMetadata viewVersionMetadata) {
    if (viewVersionMetadata != null) {
      return viewVersionMetadata.history().get(0).timestampMillis();
    }
    return 0L;
  }

  private static String getViewSqlDefinition(ViewVersionMetadata viewVersionMetadata) {
    if (viewVersionMetadata != null) {
      return viewVersionMetadata.definition().sql();
    }
    return "";
  }

  private static String getViewSqlContext(ViewVersionMetadata viewVersionMetadata) {
    if (viewVersionMetadata != null) {
      return viewVersionMetadata.definition().sessionNamespace().toString();
    }
    return "";
  }

  private Optional<DataplaneTableInfo> dataplaneTableInfoRetriever(ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    try {
      List<String> catalogKey = entry.getNameElements();
      String metadataLocation = nessieClient.getMetadataLocation(catalogKey, resolvedVersionContext, null);
      EntityPath keyPath = toEntityPath(entry);
      Table table = getIcebergTable(keyPath, metadataLocation);
      String tableId = nessieClient.getContentId(catalogKey, resolvedVersionContext, null);
      Optional<String> schemaId = determineSchemaId(entry, resolvedVersionContext);

      return Optional.of(new DataplaneTableInfo.newBuilder()
        .tableId(tableId != null ? tableId : "")
        .sourceId(this.getId().getConfig().getId().getId())
        .name(entry.getName())
        .schema(schemaId.orElse(""))
        .path(keyPath.toString())
        .tag(getUUIDFromMetadataLocation(metadataLocation))
        .formatType(entry.getType())
        .createdAt(table.history().get(0).timestampMillis())
        .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllTableInfo", e);
      // There is no way to propagate an error in SYS.TABLES queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  @Override
  @WithSpan
  public Stream<DataplaneTableInfo> getAllTableInfo() {
    ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
    return listAllEntries(resolvedVersionContext, EnumSet.of(Type.ICEBERG_TABLE))
      .map(entry -> dataplaneTableInfoRetriever(entry, resolvedVersionContext))
      .filter(Optional::isPresent)
      .map(Optional::get);
  }

  /**
   * @return Stream of Tables.
   * If celFilter == null it means that we have nothing to find(SearchQuery) so call listEntriesIncludeNested with no filter
   * If celFilter == null && searchQuery != null then, we have something to find(SearchQuery) but we didn't get appropriate filter. so we don't call nessie
   * If celFilter != null it means that by anyhow you got appropriate filter. So, call nessie with the filter.
   * return Stream.empty if there's nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.Table> getAllInformationSchemaTableInfo(SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaTableInfo", e);
      return Stream.empty();
    }

    if(celFilter == null && searchQuery != null) {
      return Stream.empty();
    }
    try {
      ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
      return listAllEntries(resolvedVersionContext,
        EnumSet.of(Type.ICEBERG_TABLE, Type.ICEBERG_VIEW), celFilter)
        .map(this::informationSchemaTableInfoRetriever)
        .filter(Optional::isPresent)
        .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported operations or etc, the query should be ignored
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaTableInfo", e);
      return Stream.empty();
    }
  }

  private Optional<com.dremio.service.catalog.Table> informationSchemaTableInfoRetriever(ExternalNamespaceEntry entry) {
    try {
      return Optional.of(
        com.dremio.service.catalog.Table.newBuilder()
            .setCatalogName(DEFAULT_CATALOG_NAME)
            .setSchemaName(joinPathExcludeEntryWithDots(entry))
            .setTableName(entry.getName())
            .setTableType(entry.getType() == Type.ICEBERG_TABLE ? TableType.TABLE : TableType.VIEW)
            .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling informationSchemaTableInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  /**
   * @return Stream of Views.
   * If celFilter == null it means that we have nothing to find(SearchQuery) so call listEntriesIncludeNested with no filter
   * If celFilter == null && searchQuery != null then, we have something to find(SearchQuery) but we didn't get appropriate filter. so we don't call nessie
   * If celFilter != null it means that by anyhow you got appropriate filter. So, call nessie with the filter.
   * return Stream.empty if there's nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.View> getAllInformationSchemaViewInfo(SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaViewInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
      return listAllEntries(resolvedVersionContext, EnumSet.of(Type.ICEBERG_VIEW), celFilter)
        .map(entry -> informationSchemaViewInfoRetriever(entry, resolvedVersionContext))
        .filter(Optional::isPresent)
        .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported operations or etc, the query should be ignored
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaViewInfo", e);
      return Stream.empty();
    }
  }

  private Optional<com.dremio.service.catalog.View> informationSchemaViewInfoRetriever(ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    try {
      String metadataLocation = nessieClient.getMetadataLocation(entry.getNameElements(),
        resolvedVersionContext, null);

      ViewVersionMetadata viewVersionMetadata = readViewMetadata(metadataLocation);

      return Optional.of(
        com.dremio.service.catalog.View.newBuilder()
            .setCatalogName(DEFAULT_CATALOG_NAME)
            .setSchemaName(joinPathExcludeEntryWithDots(entry))
            .setTableName(entry.getName())
            .setViewDefinition(getViewSqlDefinition(viewVersionMetadata))
            .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling informationSchemaViewInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  /**
   * @return Stream of Columns.
   * If celFilter == null it means that we have nothing to find(SearchQuery) so call listEntriesIncludeNested with no filter
   * If celFilter == null && searchQuery != null then, we have something to find(SearchQuery) but we didn't get appropriate filter. so we don't call nessie
   * If celFilter != null it means that by anyhow you got appropriate filter. So, call nessie with the filter.
   * return Stream.empty if there's nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.TableSchema> getAllInformationSchemaColumnInfo(SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
      return listAllEntries(resolvedVersionContext,
        EnumSet.of(Type.ICEBERG_TABLE, Type.ICEBERG_VIEW), celFilter)
        .map(entry -> informationSchemaColumnInfoRetriever(entry, resolvedVersionContext))
        .filter(Optional::isPresent)
        .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported operations or etc, the query should be ignored
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }
  }

  private Optional<com.dremio.service.catalog.TableSchema> informationSchemaColumnInfoRetriever(ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    try {
      String metadataLocation = nessieClient.getMetadataLocation(entry.getNameElements(), resolvedVersionContext, null);
      Schema schema;
      if (entry.getType() == Type.ICEBERG_TABLE) {
        EntityPath keyPath = toEntityPath(entry);
        Table table = getIcebergTable(keyPath, metadataLocation);
        schema = table.schema();
      } else if (entry.getType() == Type.ICEBERG_VIEW) {
        ViewVersionMetadata viewVersionMetadata = readViewMetadata(metadataLocation);
        schema = viewVersionMetadata.definition().schema();
      } else {
        throw new IllegalArgumentException("Unsupported entry type: " + entry.getType());
      }

      return Optional.of(
        com.dremio.service.catalog.TableSchema.newBuilder()
            .setCatalogName(DEFAULT_CATALOG_NAME)
            .setSchemaName(joinPathExcludeEntryWithDots(entry))
            .setTableName(entry.getName())
            .setBatchSchema(serializeIcebergSchema(schema))
            .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling informationSchemaColumnInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  private SchemaConverter newIcebergSchemaConverter() {
    return SchemaConverter.getBuilder()
      .setMapTypeEnabled(context.getOptionManager().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
      .build();
  }

  private ByteString serializeIcebergSchema(Schema icebergSchema) {
    SchemaConverter converter = newIcebergSchemaConverter();
    return ByteString.copyFrom(converter.fromIceberg(icebergSchema).serialize());
  }

  /**
   * @return Stream of Schemata.
   * If celFilter == null it means that we have nothing to find(SearchQuery) so call listEntriesIncludeNested with no filter
   * If celFilter == null && searchQuery != null then, we have something to find(SearchQuery) but we didn't get appropriate filter. so we don't call nessie
   * If celFilter != null it means that by anyhow you got appropriate filter. So, call nessie with the filter.
   * return Stream.empty if there's nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.Schema> getAllInformationSchemaSchemataInfo(SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, true, name);
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = nessieClient.getDefaultBranch();
      return listAllEntries(resolvedVersionContext, EnumSet.of(Type.FOLDER), celFilter)
        .map(this::informationSchemaSchemataInfoRetriever)
        .filter(Optional::isPresent)
        .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported operations or etc, the query should be ignored
      logger.warn("Failed to retrieve information while calling getAllInformationSchemaSchemataInfo", e);
      return Stream.empty();
    }
  }

  @Override
  public void close() {
    AutoCloseables.close(
      new RuntimeException("Error while closing DataplanePlugin."),
      nessieClient,
      super::close);
  }

  private Optional<com.dremio.service.catalog.Schema> informationSchemaSchemataInfoRetriever(ExternalNamespaceEntry entry) {
    try {
      return Optional.of(
        com.dremio.service.catalog.Schema.newBuilder()
            .setCatalogName(DEFAULT_CATALOG_NAME)
            .setSchemaName(joinPathIncludeEntryWithDots(entry))
            .setSchemaOwner("<owner>")
            .setSchemaType(SchemaType.SIMPLE)
            .setIsMutable(false)
            .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling informationSchemaSchemataInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  private String joinPathExcludeEntryWithDots(ExternalNamespaceEntry entry) {
    List<String> path = new ArrayList<>();
    path.add(name);
    path.addAll(entry.getNamespace());
    return DOT_JOINER.join(path);
  }

  private String joinPathIncludeEntryWithDots(ExternalNamespaceEntry entry) {
    List<String> path = new ArrayList<>();
    path.add(name);
    path.addAll(entry.getNameElements());
    return DOT_JOINER.join(path);
  }

  private EntityPath toEntityPath(ExternalNamespaceEntry entry) {
    List<String> path = new ArrayList<>();
    path.add(name);
    path.addAll(entry.getNameElements());
    return new EntityPath(path);
  }

  private Stream<ExternalNamespaceEntry> listAllEntries(
    ResolvedVersionContext resolvedVersionContext,
    Set<ExternalNamespaceEntry.Type> contentTypeFilter
  ) {
    return listAllEntries(resolvedVersionContext, contentTypeFilter, null);
  }

  private Stream<ExternalNamespaceEntry> listAllEntries(
    ResolvedVersionContext resolvedVersionContext,
    Set<ExternalNamespaceEntry.Type> contentTypeFilter,
    @Nullable String celFilter
  ) {
    return nessieClient
      .listEntries(null, resolvedVersionContext,
        NestingMode.INCLUDE_NESTED, contentTypeFilter, celFilter);
  }

  private FileIO getFileIO() {
    if (fileIO == null) {
      FileSystem fs = getSystemUserFS();
      Preconditions.checkState(fs != null,
          "Plugin must be started before accessing the DremioFileIO instance");
      fileIO = createIcebergFileIO(fs, null, null, null, null);
    }
    return fileIO;
  }
}
