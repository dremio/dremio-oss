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

import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AWS_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_AZURE_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_GCS_STORAGE_ENABLED;
import static com.dremio.exec.store.DataplanePluginOptions.DATAPLANE_LOCAL_FILE_SYSTEM_ENABLED;
import static com.dremio.exec.store.iceberg.IcebergUtils.DEFAULT_TABLE_PROPERTIES;
import static com.dremio.plugins.dataplane.store.InformationSchemaCelFilter.getInformationSchemaFilter;

import com.dremio.catalog.model.CatalogEntityKey;
import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.catalog.model.VersionContext;
import com.dremio.catalog.model.VersionedDatasetId;
import com.dremio.catalog.model.dataset.TableVersionContext;
import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.concurrent.ContextAwareCompletableFuture;
import com.dremio.common.concurrent.bulk.BulkRequest;
import com.dremio.common.concurrent.bulk.BulkResponse;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.EntityPathWithOptions;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.options.TimeTravelOption;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.DataplaneTableInfo;
import com.dremio.exec.catalog.DataplaneViewInfo;
import com.dremio.exec.catalog.ImmutableVersionedListResponsePage;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.VersionedListOptions;
import com.dremio.exec.catalog.VersionedListResponsePage;
import com.dremio.exec.catalog.VersionedPlugin;
import com.dremio.exec.dotfile.View;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ViewOptions;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.BulkSourceMetadata;
import com.dremio.exec.store.ChangeInfo;
import com.dremio.exec.store.NessieConnectionProvider;
import com.dremio.exec.store.NoDefaultBranchException;
import com.dremio.exec.store.ReferenceConflictException;
import com.dremio.exec.store.ReferenceInfo;
import com.dremio.exec.store.ReferenceNotFoundException;
import com.dremio.exec.store.ReferenceTypeConflictException;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.VersionedDatasetAccessOptions;
import com.dremio.exec.store.dfs.AddPrimaryKey;
import com.dremio.exec.store.dfs.CreateParquetTableEntry;
import com.dremio.exec.store.dfs.DropPrimaryKey;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemRulesFactory;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.MetadataIOPool;
import com.dremio.exec.store.iceberg.FieldIdBroker;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.exec.store.iceberg.IcebergViewMetadata;
import com.dremio.exec.store.iceberg.IcebergViewMetadataUtils;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.exec.store.iceberg.TimeTravelProcessors;
import com.dremio.exec.store.iceberg.VersionedUdfMetadata;
import com.dremio.exec.store.iceberg.VersionedUdfMetadataImpl;
import com.dremio.exec.store.iceberg.VersionedUdfMetadataUtils;
import com.dremio.exec.store.iceberg.ViewHandle;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.Udf;
import com.dremio.exec.store.iceberg.dremioudf.api.udf.UdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.ImmutableUdfSignature;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfMetadataParser;
import com.dremio.exec.store.iceberg.dremioudf.core.udf.UdfUtil;
import com.dremio.exec.store.iceberg.model.IcebergCommitOrigin;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieFilePathSanitizer;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedModel;
import com.dremio.exec.store.iceberg.nessie.IcebergNessieVersionedTableIdentifier;
import com.dremio.exec.store.iceberg.nessie.IcebergViewOperationsBuilder;
import com.dremio.exec.store.iceberg.nessie.IcebergViewOperationsImpl;
import com.dremio.exec.store.iceberg.nessie.VersionedUdfBuilder;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.sys.udf.UserDefinedFunction;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.nessiemetadata.cache.NessieDataplaneCache;
import com.dremio.nessiemetadata.cache.NessieDataplaneCacheProvider;
import com.dremio.nessiemetadata.cacheLoader.DataplaneCacheLoader;
import com.dremio.nessiemetadata.storeprovider.NessieDataplaneCacheStoreProvider;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.BooleanValidator;
import com.dremio.plugins.ExternalNamespaceEntry;
import com.dremio.plugins.ExternalNamespaceEntry.Type;
import com.dremio.plugins.ImmutableNessieListOptions;
import com.dremio.plugins.MergeBranchOptions;
import com.dremio.plugins.NessieClient;
import com.dremio.plugins.NessieClient.ContentMode;
import com.dremio.plugins.NessieClient.NestingMode;
import com.dremio.plugins.NessieContent;
import com.dremio.plugins.NessieListOptions;
import com.dremio.plugins.NessieListResponsePage;
import com.dremio.plugins.NessieTableAdapter;
import com.dremio.plugins.util.ContainerAccessDeniedException;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.SearchQuery;
import com.dremio.service.catalog.TableType;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.function.proto.FunctionArg;
import com.dremio.service.namespace.function.proto.FunctionBody;
import com.dremio.service.namespace.function.proto.FunctionConfig;
import com.dremio.service.namespace.function.proto.FunctionDefinition;
import com.dremio.service.namespace.function.proto.ReturnType;
import com.dremio.service.namespace.proto.EntityId;
import com.dremio.telemetry.api.metrics.MetricsInstrumenter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.inject.Provider;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.types.Types;
import org.jetbrains.annotations.NotNull;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.error.NessieForbiddenException;
import org.projectnessie.model.MergeResponse;

/** Plugin to represent Dremio Dataplane (DDP) Catalog in Dremio Query Engine (DQE). */
public abstract class DataplanePlugin extends FileSystemPlugin<AbstractDataplanePluginConfig>
    implements VersionedPlugin, MutablePlugin, NessieConnectionProvider, BulkSourceMetadata {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(DataplanePlugin.class);

  @SuppressWarnings("checkstyle:ConstantName")
  private static final MetricsInstrumenter metrics = new MetricsInstrumenter(DataplanePlugin.class);

  private static final String DEFAULT_CATALOG_NAME = "DREMIO";
  private final AbstractDataplanePluginConfig pluginConfig;
  private final SabotContext context;
  private final String name;

  /**
   * The cached DremioFileIO instance for the plugin. This is created on-demand - consumers should
   * access this only via the getFileIO() method which handles the creation.
   */
  private FileIO fileIO;

  private boolean pluginClosed = false;
  private String pluginCloseStacktrace = null;
  private static final Joiner DOT_JOINER = Joiner.on('.');
  private final NessieDataplaneCache<String, TableMetadata> tableLoadingCache;
  private final NessieDataplaneCache<String, IcebergViewMetadata> viewLoadingCache;
  private final NessieDataplaneCache<String, VersionedUdfMetadata> udfLoadingCache;

  public DataplanePlugin(
      AbstractDataplanePluginConfig pluginConfig,
      SabotContext context,
      String name,
      Provider<StoragePluginId> idProvider,
      NessieDataplaneCacheProvider cacheProvider,
      @Nullable NessieDataplaneCacheStoreProvider nessieDataplaneCacheStoreProvider) {
    super(pluginConfig, context, name, idProvider);
    this.pluginConfig = pluginConfig;
    this.context = context;
    this.name = name;

    OptionManager optionManager = context.getOptionManager();
    this.tableLoadingCache =
        cacheProvider.get(optionManager, new TableCacheLoader(), nessieDataplaneCacheStoreProvider);
    this.viewLoadingCache =
        cacheProvider.get(optionManager, new ViewCacheLoader(), nessieDataplaneCacheStoreProvider);
    this.udfLoadingCache =
        cacheProvider.get(optionManager, new UdfCacheLoader(), nessieDataplaneCacheStoreProvider);
  }

  @Override
  public ResolvedVersionContext resolveVersionContext(VersionContext versionContext) {
    ResolvedVersionContext resolvedVersionContext =
        getNessieClient().resolveVersionContext(versionContext);
    logger.debug("VersionContext '{}' resolved to '{}'", versionContext, resolvedVersionContext);
    return resolvedVersionContext;
  }

  @Override
  @WithSpan
  public boolean commitExists(String commitHash) {
    return metrics.log("commit_exists", () -> getNessieClient().commitExists(commitHash));
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listBranches() {
    return metrics.log("list_branches", getNessieClient()::listBranches);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listTags() {
    return metrics.log("list_tags", getNessieClient()::listTags);
  }

  @Override
  @WithSpan
  public Stream<ReferenceInfo> listReferences() {
    return metrics.log("list_references", getNessieClient()::listReferences);
  }

  @Override
  @WithSpan
  public Stream<ChangeInfo> listChanges(VersionContext version) {
    return metrics.log("list_changes", () -> getNessieClient().listChanges(version));
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listEntries(
      List<String> catalogPath, VersionContext version) {
    return metrics.log(
        "list_entries",
        () -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
          return getNessieClient()
              .listEntries(
                  catalogPath,
                  resolvedVersion,
                  NestingMode.IMMEDIATE_CHILDREN_ONLY,
                  ContentMode.ENTRY_METADATA_ONLY,
                  null,
                  null);
        });
  }

  @Override
  @WithSpan
  public VersionedListResponsePage listEntriesPage(
      List<String> catalogPath, VersionContext version, VersionedListOptions options)
      throws ReferenceNotFoundException, NoDefaultBranchException, ReferenceConflictException {
    return metrics.log(
        "list_entries_page",
        () -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
          return convertNessieListResponse(
              getNessieClient()
                  .listEntriesPage(
                      catalogPath,
                      resolvedVersion,
                      NestingMode.IMMEDIATE_CHILDREN_ONLY,
                      ContentMode.ENTRY_METADATA_ONLY,
                      null,
                      null,
                      convertListOptions(options)));
        });
  }

  @VisibleForTesting
  @Override
  public Stream<ExternalNamespaceEntry> listEntriesIncludeNested(
      List<String> catalogPath, VersionContext version) {
    return metrics.log(
        "list_entries_include_nested",
        () -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
          return getNessieClient()
              .listEntries(
                  catalogPath,
                  resolvedVersion,
                  NestingMode.INCLUDE_NESTED_CHILDREN,
                  ContentMode.ENTRY_METADATA_ONLY,
                  null,
                  null);
        });
  }

  private static NessieListOptions convertListOptions(VersionedListOptions options) {
    ImmutableNessieListOptions.Builder builder =
        new ImmutableNessieListOptions.Builder().setPageToken(options.pageToken());
    if (options.maxResultsPerPage().isPresent()) {
      builder.setMaxResultsPerPage(options.maxResultsPerPage().getAsInt());
    }
    return builder.build();
  }

  private static VersionedListResponsePage convertNessieListResponse(
      NessieListResponsePage response) {
    return new ImmutableVersionedListResponsePage.Builder()
        .setEntries(response.entries())
        .setPageToken(response.pageToken())
        .build();
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listTablesIncludeNested(
      List<String> catalogPath, VersionContext version) {
    return metrics.log(
        "list_tables_include_nested",
        () -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
          return getNessieClient()
              .listEntries(
                  catalogPath,
                  resolvedVersion,
                  NestingMode.INCLUDE_NESTED_CHILDREN,
                  ContentMode.ENTRY_METADATA_ONLY,
                  EnumSet.of(Type.ICEBERG_TABLE),
                  null);
        });
  }

  @Override
  @WithSpan
  public Stream<ExternalNamespaceEntry> listViewsIncludeNested(
      List<String> catalogPath, VersionContext version) {
    return metrics.log(
        "list_views_include_nested",
        () -> {
          ResolvedVersionContext resolvedVersion = resolveVersionContext(version);
          return getNessieClient()
              .listEntries(
                  catalogPath,
                  resolvedVersion,
                  NestingMode.INCLUDE_NESTED_CHILDREN,
                  ContentMode.ENTRY_METADATA_ONLY,
                  EnumSet.of(Type.ICEBERG_VIEW),
                  null);
        });
  }

  @Override
  @WithSpan
  public void createNamespace(NamespaceKey namespaceKey, VersionContext version) {
    logger.debug("Creating namespace '{}' from '{}'", namespaceKey, version);
    metrics.log(
        "create_namespace",
        () ->
            getNessieClient()
                .createNamespace(schemaComponentsWithoutPluginName(namespaceKey), version));
  }

  @Override
  @WithSpan
  public void deleteFolder(NamespaceKey namespaceKey, VersionContext version) {
    logger.debug("Deleting Folder '{}' from '{}'", namespaceKey, version);
    metrics.log(
        "delete_folder",
        () ->
            getNessieClient()
                .deleteNamespace(schemaComponentsWithoutPluginName(namespaceKey), version));
  }

  @Override
  @WithSpan
  public void createBranch(String branchName, VersionContext sourceVersion) {
    logger.debug("Creating branch '{}' from '{}'", branchName, sourceVersion);
    metrics.log("create_branch", () -> getNessieClient().createBranch(branchName, sourceVersion));
  }

  @Override
  @WithSpan
  public void createTag(String tagName, VersionContext sourceVersion) {
    logger.debug("Creating tag '{}' from '{}'", tagName, sourceVersion);
    metrics.log("create_tag", () -> getNessieClient().createTag(tagName, sourceVersion));
  }

  @Override
  @WithSpan
  public void dropBranch(String branchName, String branchHash) {
    logger.debug("Drop branch '{}' at '{}'", branchName, branchHash);
    metrics.log("drop_branch", () -> getNessieClient().dropBranch(branchName, branchHash));
  }

  @Override
  @WithSpan
  public void dropTag(String tagName, String tagHash) {
    logger.debug("Dropping tag '{}' at '{}'", tagName, tagHash);
    metrics.log("drop_tag", () -> getNessieClient().dropTag(tagName, tagHash));
  }

  @Override
  @WithSpan
  public MergeResponse mergeBranch(
      String sourceBranchName, String targetBranchName, MergeBranchOptions mergeBranchOptions) {
    logger.debug(
        "Merging branch '{}' into '{}'. mergeBranchOptions '{}'",
        sourceBranchName,
        targetBranchName,
        mergeBranchOptions);
    return metrics.log(
        "merge_branch",
        () ->
            getNessieClient().mergeBranch(sourceBranchName, targetBranchName, mergeBranchOptions));
  }

  @Override
  @WithSpan
  public void assignBranch(String branchName, VersionContext sourceVersion)
      throws ReferenceConflictException, ReferenceNotFoundException {
    logger.debug("Assign branch '{}' to {}", branchName, sourceVersion);
    metrics.log("assign_branch", () -> getNessieClient().assignBranch(branchName, sourceVersion));
  }

  @Override
  @WithSpan
  public void assignTag(String tagName, VersionContext sourceVersion)
      throws ReferenceConflictException, ReferenceNotFoundException {
    logger.debug("Assign tag '{}' to {}", tagName, sourceVersion);
    metrics.log("assign_tag", () -> getNessieClient().assignTag(tagName, sourceVersion));
  }

  @Override
  @WithSpan
  public Optional<DatasetHandle> getDatasetHandle(
      EntityPath datasetPath, GetDatasetOption... options) {
    try {
      return metrics.log("get_dataset_handle", () -> getDatasetHandleHelper(datasetPath, options));
    } catch (NessieForbiddenException e) {
      throw new AccessControlException(e.getMessage());
    }
  }

  @Override
  @WithSpan
  public BulkResponse<EntityPathWithOptions, Optional<DatasetHandle>> bulkGetDatasetHandles(
      BulkRequest<EntityPathWithOptions> requestedDatasets) {
    MetadataIOPool metadataIOPool = context.getMetadataIOPool();
    return requestedDatasets.handleRequests(
        dataset ->
            ContextAwareCompletableFuture.createFrom(
                metadataIOPool.execute(
                    new MetadataIOPool.MetadataTask<>(
                        "bulk_get_dataset_handles_async",
                        dataset.entityPath(),
                        () -> {
                          try {
                            return metrics.log(
                                "bulkGetDatasetHandles-asyncGet",
                                () ->
                                    getDatasetHandleHelper(
                                        dataset.entityPath(), dataset.options()));
                          } catch (NessieForbiddenException ex) {
                            throw new AccessControlException(ex.getMessage());
                          }
                        }))));
  }

  private Optional<DatasetHandle> getDatasetHandleHelper(
      EntityPath datasetPath, GetDatasetOption[] options) {
    final ResolvedVersionContext version =
        Preconditions.checkNotNull(
            VersionedDatasetAccessOptions.getVersionedDatasetAccessOptions(options)
                .getVersionContext());
    logger.debug("Getting dataset handle for '{}' at version {} ", datasetPath, version);

    List<String> versionedTableKey = datasetPath.getComponents().subList(1, datasetPath.size());
    Optional<NessieContent> maybeNessieContent =
        getNessieClient().getContent(versionedTableKey, version, null);
    if (maybeNessieContent.isEmpty()) {
      return Optional.empty();
    }
    NessieContent nessieContent = maybeNessieContent.get();
    final EntityType entityType = nessieContent.getEntityType();
    switch (entityType) {
      case ICEBERG_TABLE:
      case ICEBERG_VIEW:
        break;
      default:
        return Optional.empty();
    }
    final String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      return Optional.empty();
    }

    final String contentId = nessieContent.getContentId();
    final String uniqueId = getUUIDFromMetadataLocation(metadataLocation);

    switch (entityType) {
      case ICEBERG_TABLE:
        final Table table = getIcebergTable(datasetPath, metadataLocation, version);
        logger.debug(
            "Retrieved Iceberg table : name {} , location {}, schema {}, current snapshot {}, partition spec {} ",
            table.name(),
            table.location(),
            table.schema(),
            table.currentSnapshot(),
            table.spec());

        final TimeTravelOption travelOption = TimeTravelOption.getTimeTravelOption(options);
        final TimeTravelOption.TimeTravelRequest timeTravelRequest =
            travelOption != null ? travelOption.getTimeTravelRequest() : null;
        final TableSnapshotProvider tableSnapshotProvider =
            TimeTravelProcessors.getTableSnapshotProvider(
                datasetPath.getComponents(), timeTravelRequest);
        logger.debug("Time travel request {} ", timeTravelRequest);
        final TableSchemaProvider tableSchemaProvider =
            TimeTravelProcessors.getTableSchemaProvider(timeTravelRequest);
        return Optional.of(
            new TransientIcebergMetadataProvider(
                datasetPath,
                Suppliers.ofInstance(table),
                getFsConfCopy(),
                tableSnapshotProvider,
                this,
                tableSchemaProvider,
                context.getOptionManager(),
                contentId,
                uniqueId));

      case ICEBERG_VIEW:
        final IcebergViewMetadata icebergViewMetadata = getIcebergView(metadataLocation);

        return Optional.of(
            ViewHandle.newBuilder()
                .datasetpath(datasetPath)
                .icebergViewMetadata(icebergViewMetadata)
                .id(contentId)
                .uniqueId(uniqueId)
                .build());

      default:
        throw new IllegalStateException("Unsupported entityType: " + entityType);
    }
  }

  private String getUUIDFromMetadataLocation(String metadataLocation) {
    return metadataLocation.substring(
        metadataLocation.lastIndexOf("/") + 1, metadataLocation.lastIndexOf(".metadata.json"));
  }

  @Override
  @WithSpan
  public PartitionChunkListing listPartitionChunks(
      DatasetHandle datasetHandle, ListPartitionChunkOption... options) {
    return metrics.log(
        "list_partition_chunks",
        () -> {
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
      GetMetadataOption... options) {
    return metrics.log(
        "get_dataset_metadata",
        () -> {
          TransientIcebergMetadataProvider icebergMetadataProvider =
              datasetHandle.unwrap(TransientIcebergMetadataProvider.class);
          return icebergMetadataProvider.getDatasetMetadata(options);
        });
  }

  @Override
  public boolean containerExists(EntityPath containerPath, GetMetadataOption... options) {
    final ResolvedVersionContext resolvedVersionContext =
        Preconditions.checkNotNull(
            VersionedDatasetAccessOptions.getVersionedDatasetAccessOptions(options)
                .getVersionContext());
    return folderExists(
        containerPath.getComponents().subList(1, containerPath.size()), resolvedVersionContext);
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    throw new UnsupportedOperationException("Views aren't supported");
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return context
        .getConfig()
        .getClass(
            "dremio.plugins.dfs.rulesfactory",
            StoragePluginRulesFactory.class,
            FileSystemRulesFactory.class);
  }

  @Override
  @WithSpan
  public void createEmptyTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      BatchSchema batchSchema,
      WriterOptions writerOptions) {
    metrics.log(
        "create_empty_table",
        () -> createEmptyTableHelper(tableSchemaPath, schemaConfig, batchSchema, writerOptions));
  }

  private void createEmptyTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      BatchSchema batchSchema,
      WriterOptions writerOptions) {
    final ResolvedVersionContext version = Preconditions.checkNotNull(writerOptions.getVersion());

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);

    validateEntityOfOtherTypeDoesNotExist(
        tableSchemaComponentsWithoutPluginName,
        writerOptions.getVersion(),
        EntityType.ICEBERG_TABLE);

    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null, // Used to create DremioInputFile. (valid only for insert/ctas)
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());

    logger.debug(
        "Creating empty table: '{}' with version '{}'",
        tableSchemaComponentsWithoutPluginName,
        version);
    try {
      PartitionSpec partitionSpec =
          Optional.ofNullable(
                  writerOptions
                      .getTableFormatOptions()
                      .getIcebergSpecificOptions()
                      .getIcebergTableProps())
              .map(IcebergTableProps::getDeserializedPartitionSpec)
              .orElse(null);
      Map<String, String> tableProperties =
          Optional.ofNullable(
                  writerOptions
                      .getTableFormatOptions()
                      .getIcebergSpecificOptions()
                      .getIcebergTableProps())
              .map(IcebergTableProps::getTableProperties)
              .orElse(Collections.emptyMap());
      Map<String, String> tablePropertiesWithDefault =
          tableProperties.isEmpty() ? new HashMap<>() : tableProperties;
      tablePropertiesWithDefault.putAll(DEFAULT_TABLE_PROPERTIES);

      IcebergTableIdentifier tableIdentifier = icebergModel.getTableIdentifier(getRootLocation());
      icebergModel
          .getCreateTableCommitter(
              String.join(".", tableSchemaComponentsWithoutPluginName),
              tableIdentifier,
              batchSchema,
              writerOptions.getPartitionColumns(),
              null,
              partitionSpec,
              writerOptions.getDeserializedSortOrder(),
              tablePropertiesWithDefault,
              getNewTableLocation((IcebergNessieVersionedTableIdentifier) tableIdentifier))
          .commit();
    } catch (UncheckedIOException e) {
      if (e.getCause() instanceof ContainerAccessDeniedException) {
        throw UserException.permissionError(e.getCause())
            .message("Access denied while creating table. %s", e.getMessage())
            .buildSilently();
      }
      throw e;
    }
  }

  private String getNewTableLocation(IcebergNessieVersionedTableIdentifier tableIdentifier) {
    return tableIdentifier.getTableLocation() + "_" + UUID.randomUUID();
  }

  @Override
  @WithSpan
  public CreateTableEntry createNewTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions,
      Map<String, Object> storageOptions,
      boolean isResultsTable) {
    return metrics.log(
        "create_new_table",
        () ->
            createNewTableHelper(tableSchemaPath, schemaConfig, icebergTableProps, writerOptions));
  }

  private CreateTableEntry createNewTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      IcebergTableProps icebergTableProps,
      WriterOptions writerOptions) {
    Preconditions.checkNotNull(icebergTableProps);
    Preconditions.checkNotNull(icebergTableProps.getVersion());
    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    final String tableName = PathUtils.constructFullPath(tableSchemaComponentsWithoutPluginName);
    final String userName = schemaConfig.getUserName();

    validateEntityOfOtherTypeDoesNotExist(
        tableSchemaComponentsWithoutPluginName,
        writerOptions.getVersion(),
        EntityType.ICEBERG_TABLE);

    Path path = resolveTableNameToValidPath(tableSchemaPath.toString(), writerOptions.getVersion());
    icebergTableProps = new IcebergTableProps(icebergTableProps);
    icebergTableProps.setTableLocation(path.toString());
    icebergTableProps.setTableName(tableName);
    Map<String, String> tablePropertiesWithDefault =
        (icebergTableProps.getTableProperties().isEmpty())
            ? new HashMap()
            : icebergTableProps.getTableProperties();
    tablePropertiesWithDefault.putAll(DEFAULT_TABLE_PROPERTIES);
    icebergTableProps.setTableProperties(tablePropertiesWithDefault);
    Preconditions.checkState(
        icebergTableProps.getUuid() != null && !icebergTableProps.getUuid().isEmpty(),
        "Unexpected state. UUID must be set");
    path = path.resolve(icebergTableProps.getUuid());
    logger.debug(
        "Creating new table '{}' with options '{}' IcebergTableProps  '{}' ",
        tableSchemaPath,
        writerOptions,
        icebergTableProps);
    return new CreateParquetTableEntry(
        userName,
        getNessieCommitUserId(),
        this, // This requires FSPlugin features
        path.toString(),
        icebergTableProps,
        writerOptions,
        tableSchemaPath,
        null);
  }

  /**
   * Resolve given table path relative to source resolve it to a valid path in filesystem. If the
   * table exists, fetch the path from the versioned store. If not, resolve under base location. If
   * the resolved path refers to an entity not under the base of the source then a permission error
   * is thrown.
   */
  private Path resolveTableNameToValidPath(
      String tablePathWithPlugin, ResolvedVersionContext versionContext) {
    List<String> tableComponents =
        schemaComponentsWithoutPluginName(
            new NamespaceKey(PathUtils.parseFullPath(tablePathWithPlugin)));
    List<String> objectStoragePath = getIcebergNessieFilePathSanitizer().getPath(tableComponents);
    Optional<String> metadataLocation = getMetadataLocation(tableComponents, versionContext);
    logger.info("Retrieving Iceberg metadata from location '{}' ", metadataLocation);

    if (metadataLocation.isEmpty()) {
      // Table does not exist, resolve new path under the aws root folder location
      // location where the iceberg table folder will be created
      // Format : "<plugin.s3RootPath>"/"<folder1>/<folder2>/<tableName>_uuid"
      Path basePath = pluginConfig.getPath();
      String relativePathClean = PathUtils.removeLeadingSlash(String.join("/", objectStoragePath));
      String relativePathWithRandomUuid = relativePathClean + addUniqueSuffix();
      Path combined = basePath.resolve(relativePathWithRandomUuid);
      PathUtils.verifyNoDirectoryTraversal(
          objectStoragePath,
          () ->
              UserException.permissionError()
                  .message("Not allowed to perform directory traversal")
                  .addContext("Path", objectStoragePath.toString())
                  .buildSilently());
      PathUtils.verifyNoAccessOutsideBase(basePath, combined);
      return combined;
    }

    final Table icebergTable =
        getIcebergTable(new EntityPath(objectStoragePath), metadataLocation.get(), versionContext);
    return Path.of(fixupIcebergTableLocation(icebergTable.location()));
  }

  private String fixupIcebergTableLocation(String location) {
    return removeUriScheme(removeAzureHost(location));
  }

  /** Removes URI scheme (e.g. "s3a://bucket/folder" -> "/bucket/folder") */
  private static String removeUriScheme(String location) {
    if (StringUtils.isBlank(location)) {
      return location;
    }

    int urlSchemeIndex = location.indexOf("://");
    if (urlSchemeIndex > 0) {
      location = location.substring(urlSchemeIndex + 2);
    }
    return location;
  }

  /**
   * TODO DX-83378: Better support for other Azure schemes/URLs
   *
   * <p>Converts an Azure Storage URI to relative path using just the container name.
   *
   * <p>Note that this is lossy, the Storage Account name is omitted entirely.
   *
   * <p>Azure Storage URIs using wasb or wasbs schemes have this format:
   * wasb://<containername>@<accountname>.blob.core.windows.net/<file.path>/
   *
   * <p>For example: wasb://mycontainer@myaccount.blob.core.windows.net/folderA/folderB converts to:
   * wasb://mycontainer/folderA/folderB
   */
  protected String removeAzureHost(String location) {
    if (!StringUtils.startsWithAny(location, "wasb://", "wasbs://")) {
      return location;
    }

    if (!location.contains("@")
        || !StringUtils.containsIgnoreCase(location, "blob.core.windows.net")) {
      throw UserException.validationError()
          .message(
              String.format("Metadata location [%s] for Azure filesystem is malformed.", location))
          .buildSilently();
    }

    return StringUtils.substringBefore(location, "@")
        + StringUtils.substringAfter(location, "blob.core.windows.net");
  }

  @Override
  public Writer getWriter(
      PhysicalOperator child, String location, WriterOptions options, OpProps props) {
    throw new UnsupportedOperationException();
  }

  @Override
  @WithSpan
  public void dropTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "drop_table", () -> dropTableHelper(tableSchemaPath, schemaConfig, tableMutationOptions));
  }

  private void dropTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableKeyWithoutPluginName = schemaComponentsWithoutPluginName(tableSchemaPath);
    // Check if the entity is a table.
    Optional<NessieContent> maybeNessieContent =
        getNessieClient().getContent(tableKeyWithoutPluginName, version, null);
    if (maybeNessieContent.isEmpty()) {
      throw UserException.validationError()
          .message("%s does not exist ", tableKeyWithoutPluginName)
          .buildSilently();
    }
    NessieContent nessieContent = maybeNessieContent.get();
    EntityType entityType = nessieContent.getEntityType();
    if (entityType != EntityType.ICEBERG_TABLE) {
      throw UserException.validationError()
          .message("%s is not a TABLE ", tableKeyWithoutPluginName)
          .buildSilently();
    }
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableKeyWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // userId not needed for DDL operations
            getIcebergNessieFilePathSanitizer());

    logger.debug("Dropping table '{}' at version '{}'", tableKeyWithoutPluginName, version);
    icebergModel.deleteTable(icebergModel.getTableIdentifier(pluginConfig.getRootPath()));
  }

  @Override
  @WithSpan
  public void alterTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      AlterTableOption alterTableOption,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "update_table",
        () ->
            alterTableHelper(
                tableSchemaPath, schemaConfig, alterTableOption, tableMutationOptions));
  }

  private void alterTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      AlterTableOption alterTableOption,
      TableMutationOptions tableMutationOptions) {
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    logger.debug(
        "Altering table partition spec for table {} at version {} with options {}",
        tableSchemaComponentsWithoutPluginName,
        version,
        alterTableOption);
    icebergModel.alterTable(icebergModel.getTableIdentifier(getRootLocation()), alterTableOption);
  }

  @Override
  @WithSpan
  public void truncateTable(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "truncate_table",
        () -> truncateTableHelper(tableSchemaPath, schemaConfig, tableMutationOptions));
  }

  private void truncateTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);

    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    logger.debug(
        "Truncating table '{}' at version '{}'", tableSchemaComponentsWithoutPluginName, version);
    IcebergTableIdentifier icebergTableIdentifier =
        icebergModel.getTableIdentifier(getRootLocation());
    icebergModel.truncateTable(icebergTableIdentifier);
  }

  @Override
  @WithSpan
  public void rollbackTable(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {

    metrics.log(
        "rollback_table",
        () ->
            rollbackTableHelper(
                tableSchemaPath, schemaConfig, rollbackOption, tableMutationOptions));
  }

  private void rollbackTableHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      RollbackOption rollbackOption,
      TableMutationOptions tableMutationOptions) {
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    logger.debug(
        "Rollback table {} at version {} with options {}",
        tableSchemaComponentsWithoutPluginName,
        version,
        rollbackOption);
    icebergModel.rollbackTable(icebergModel.getTableIdentifier(getRootLocation()), rollbackOption);
  }

  @Override
  @WithSpan
  public boolean createOrUpdateView(
      NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, View view, ViewOptions viewOptions) {

    return metrics.log(
        "create_or_update_view",
        () -> createOrUpdateViewHelper(tableSchemaPath, schemaConfig, view, viewOptions));
  }

  public boolean createOrUpdateViewHelper(
      NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, View view, ViewOptions viewOptions) {
    if (!viewOptions.getVersion().isBranch()) {
      throw UserException.validationError()
          .message("Cannot update a view on a tag or bareCommit")
          .buildSilently();
    }
    if (view != null && view.hasDeclaredFieldNames()) {
      throw UserException.unsupportedError()
          .message("Versioned views don't support field aliases.")
          .buildSilently();
    }

    final ResolvedVersionContext resolvedVersionContext =
        Objects.requireNonNull(viewOptions.getVersion());
    final SchemaConverter converter = newIcebergSchemaConverter();
    final List<String> viewKey = schemaComponentsWithoutPluginName(tableSchemaPath);
    final org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(getRootLocation());

    validateEntityOfOtherTypeDoesNotExist(viewKey, resolvedVersionContext, EntityType.ICEBERG_VIEW);

    try {
      final String warehouseLocation =
          IcebergUtils.getValidIcebergPath(path, getFsConfCopy(), getSystemUserFS().getScheme());

      IcebergViewOperationsImpl icebergViewOperationsBuilder =
          IcebergViewOperationsBuilder.newViewOps()
              .withViewMetadataWarehouseLocation(warehouseLocation)
              .withCatalogName(tableSchemaPath.getRoot())
              .withFileIO(getFileIO())
              .withUserName(schemaConfig.getUserName())
              .withViewSpecVersion(viewOptions.getIcebergViewVersion())
              .withMetadataLoader(this::getIcebergView)
              .withSanitizer(getIcebergNessieFilePathSanitizer())
              .withNessieClient(getNessieClient())
              .build();

      if (viewOptions.isViewCreate()) {
        icebergViewOperationsBuilder.create(
            viewKey,
            view.getSql(),
            converter.toIcebergSchema(viewOptions.getBatchSchema()),
            view.getWorkspaceSchemaPath(),
            resolvedVersionContext);
        return true;
      }

      final String metadataLocation =
          getMetadataLocation(viewKey, resolvedVersionContext)
              .orElseThrow(
                  () ->
                      new IllegalStateException(
                          "Failed to determine metadataLocation: "
                              + viewKey
                              + " version: "
                              + resolvedVersionContext));

      final IcebergViewMetadata baseIcebergViewMetadata = getIcebergView(metadataLocation);
      final Map<String, String> currentProperties = baseIcebergViewMetadata.getProperties();

      if (viewOptions.isViewUpdate()) {
        icebergViewOperationsBuilder.update(
            viewKey,
            view.getSql(),
            converter.toIcebergSchema(viewOptions.getBatchSchema()),
            view.getWorkspaceSchemaPath(),
            currentProperties,
            resolvedVersionContext);
        return true;
      }

      if (viewOptions.isViewAlterProperties()) {
        final Map<String, String> properties = Objects.requireNonNull(viewOptions.getProperties());
        final boolean needUpdate =
            properties.entrySet().stream()
                .anyMatch(entry -> !entry.getValue().equals(currentProperties.get(entry.getKey())));

        if (!needUpdate) {
          logger.debug("No property need to be updated");
          return false;
        }

        icebergViewOperationsBuilder.update(
            viewKey,
            baseIcebergViewMetadata.getSql(),
            baseIcebergViewMetadata.getSchema(),
            baseIcebergViewMetadata.getSchemaPath(),
            properties,
            resolvedVersionContext);
        return true;
      }

    } catch (Exception ex) {
      logger.error("Exception while operating on the view", ex);
      throw ex;
    }
    return false;
  }

  @Override
  @WithSpan
  public void dropView(
      NamespaceKey tableSchemaPath, ViewOptions viewOptions, SchemaConfig schemaConfig) {
    metrics.log("drop_view", () -> dropViewHelper(tableSchemaPath, schemaConfig, viewOptions));
  }

  private void dropViewHelper(
      NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, ViewOptions viewOptions) {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(getRootLocation());
    String icebergMetadataRootPath =
        IcebergUtils.getValidIcebergPath(path, getFsConfCopy(), getSystemUserFS().getScheme());

    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps()
            .withViewMetadataWarehouseLocation(icebergMetadataRootPath)
            .withCatalogName(tableSchemaPath.getRoot())
            .withFileIO(getFileIO())
            .withUserName(schemaConfig.getUserName())
            .withViewSpecVersion(viewOptions.getIcebergViewVersion())
            .withMetadataLoader(this::getIcebergView)
            .withNessieClient(getNessieClient());

    ResolvedVersionContext version = viewOptions.getVersion();
    List<String> viewKey = schemaComponentsWithoutPluginName(tableSchemaPath);
    logger.debug("Dropping view '{}' at version '{}'", viewKey, version);
    icebergViewOperationsBuilder.build().drop(viewKey, version);
  }

  @Override
  @WithSpan
  public void addColumns(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columnsToAdd,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "add_columns",
        () -> addColumnsHelper(tableSchemaPath, schemaConfig, columnsToAdd, tableMutationOptions));
  }

  private void addColumnsHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      List<Field> columnsToAdd,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    SchemaConverter schemaConverter = newIcebergSchemaConverter();
    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    IcebergTableIdentifier icebergTableIdentifier =
        icebergModel.getTableIdentifier(getRootLocation());
    List<Types.NestedField> icebergFields = schemaConverter.toIcebergFields(columnsToAdd);

    logger.debug(
        "Adding columns '{}' to table '{}' at version '{}'",
        columnsToAdd,
        tableSchemaComponentsWithoutPluginName,
        version);
    icebergModel.addColumns(icebergTableIdentifier, icebergFields);
  }

  @Override
  @WithSpan
  public void dropColumn(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "drop_column",
        () -> dropColumnHelper(tableSchemaPath, schemaConfig, columnToDrop, tableMutationOptions));
  }

  private void dropColumnHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      String columnToDrop,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    IcebergTableIdentifier icebergTableIdentifier =
        icebergModel.getTableIdentifier(getRootLocation());
    logger.debug(
        "Dropping column '{}' for table '{}' at version '{}'",
        columnToDrop,
        tableSchemaComponentsWithoutPluginName,
        version);
    icebergModel.dropColumn(icebergTableIdentifier, columnToDrop);
  }

  @Override
  @WithSpan
  public void changeColumn(
      NamespaceKey tableSchemaPath,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      String columnToChange,
      Field fieldFromSqlColDeclaration,
      TableMutationOptions tableMutationOptions) {
    metrics.log(
        "change_column",
        () ->
            changeColumnHelper(
                tableSchemaPath,
                schemaConfig,
                columnToChange,
                fieldFromSqlColDeclaration,
                tableMutationOptions));
  }

  @Override
  @WithSpan
  public void addPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columns,
      ResolvedVersionContext versionContext) {
    metrics.log(
        "add_primary_key",
        () -> addPrimaryKeyHelper(table, datasetConfig, schemaConfig, columns, versionContext));
  }

  private void addPrimaryKeyHelper(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      List<Field> columns,
      ResolvedVersionContext resolvedVersionContext) {
    List<String> catalogKey = schemaComponentsWithoutPluginName(table);
    AddPrimaryKey op =
        new AddPrimaryKey(
            table,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModelHelper(
                catalogKey,
                resolvedVersionContext,
                null,
                schemaConfig.getUserName(),
                null,
                getFileIO()),
            validateAndGetPathForPrimaryKey(
                table, schemaConfig.getUserName(), resolvedVersionContext),
            this);
    op.performOperation(columns);
  }

  @Override
  @WithSpan
  public void dropPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext) {
    metrics.log(
        "drop_primary_key",
        () -> dropPrimaryKeyHelper(table, datasetConfig, schemaConfig, versionContext));
  }

  private void dropPrimaryKeyHelper(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext resolvedVersionContext) {
    List<String> catalogKey = schemaComponentsWithoutPluginName(table);
    DropPrimaryKey op =
        new DropPrimaryKey(
            table,
            context,
            datasetConfig,
            schemaConfig,
            getIcebergModelHelper(
                catalogKey,
                resolvedVersionContext,
                null,
                schemaConfig.getUserName(),
                null,
                getFileIO()),
            validateAndGetPathForPrimaryKey(
                table, schemaConfig.getUserName(), resolvedVersionContext),
            this);
    op.performOperation();
  }

  @Override
  @WithSpan
  public List<String> getPrimaryKey(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    return metrics.log(
        "get_primary_key",
        () ->
            getPrimaryKeyHelper(table, datasetConfig, schemaConfig, versionContext, saveInKvStore));
  }

  private List<String> getPrimaryKeyHelper(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    if (IcebergUtils.isPrimaryKeySupported(datasetConfig)) {
      return IcebergUtils.validateAndGeneratePrimaryKey(
          this, context, table, datasetConfig, schemaConfig, versionContext, saveInKvStore);
    } else {
      return null;
    }
  }

  @Override
  @WithSpan
  public List<String> getPrimaryKeyFromMetadata(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      ResolvedVersionContext versionContext,
      boolean saveInKvStore) {
    // For versioned tables, we don't cache the PK in the KV store. Grab the iceberg table
    // from the table cache.
    List<String> versionedTableKey = schemaComponentsWithoutPluginName(table);
    Optional<String> metadataLocation = getMetadataLocation(versionedTableKey, versionContext);
    logger.debug("Retrieving Iceberg metadata from location '{}' ", metadataLocation);
    if (metadataLocation.isEmpty()) {
      return null;
    }

    final Table icebergTable =
        getIcebergTable(
            new EntityPath(table.getPathComponents()), metadataLocation.get(), versionContext);
    return IcebergUtils.getPrimaryKey(icebergTable, datasetConfig);
  }

  private Path validateAndGetPathForPrimaryKey(
      NamespaceKey tableKey, String userName, ResolvedVersionContext resolvedVersionContext) {
    validate(tableKey, userName);
    return resolveTableNameToValidPath(tableKey.toString(), resolvedVersionContext);
  }

  private void changeColumnHelper(
      NamespaceKey tableSchemaPath,
      SchemaConfig schemaConfig,
      String columnToChange,
      Field fieldFromSqlColDeclaration,
      TableMutationOptions tableMutationOptions) {
    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName =
        schemaComponentsWithoutPluginName(tableSchemaPath);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConfCopy(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    IcebergTableIdentifier icebergTableIdentifier =
        icebergModel.getTableIdentifier(getRootLocation());

    logger.debug(
        "Changing column '{}' to '{}' in table '{}' at version '{}'",
        columnToChange,
        fieldFromSqlColDeclaration,
        tableSchemaComponentsWithoutPluginName,
        version);

    icebergModel.changeColumn(icebergTableIdentifier, columnToChange, fieldFromSqlColDeclaration);
  }

  @Override
  public boolean toggleSchemaLearning(
      NamespaceKey table, SchemaConfig schemaConfig, boolean enableSchemaLearning) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterSortOrder(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema batchSchema,
      SchemaConfig schemaConfig,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {

    metrics.log(
        "alter_sort_order",
        () ->
            alterSortOrderTableHelper(table, schemaConfig, sortOrderColumns, tableMutationOptions));
  }

  public void alterSortOrderTableHelper(
      NamespaceKey table,
      SchemaConfig schemaConfig,
      List<String> sortOrderColumns,
      TableMutationOptions tableMutationOptions) {

    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(table);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConf(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    logger.debug(
        "Alter Sort Order table {} at version {}", tableSchemaComponentsWithoutPluginName, version);
    icebergModel.replaceSortOrder(
        icebergModel.getTableIdentifier(getRootLocation()), sortOrderColumns);
  }

  @Override
  public void updateTableProperties(
      NamespaceKey table,
      DatasetConfig datasetConfig,
      BatchSchema schema,
      SchemaConfig schemaConfig,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {
    metrics.log(
        "update_table_properties",
        () ->
            updateTablePropertiesHelper(
                table, schemaConfig, tableProperties, tableMutationOptions, isRemove));
  }

  private void updateTablePropertiesHelper(
      NamespaceKey table,
      SchemaConfig schemaConfig,
      Map<String, String> tableProperties,
      TableMutationOptions tableMutationOptions,
      boolean isRemove) {

    Preconditions.checkNotNull(tableMutationOptions);
    final ResolvedVersionContext version = tableMutationOptions.getResolvedVersionContext();
    Preconditions.checkNotNull(version);

    List<String> tableSchemaComponentsWithoutPluginName = schemaComponentsWithoutPluginName(table);
    IcebergModel icebergModel =
        new IcebergNessieVersionedModel(
            tableSchemaComponentsWithoutPluginName,
            getFsConf(),
            getFileIO(),
            getNessieClient(),
            null,
            version,
            this,
            schemaConfig.getUserName(),
            null, // Not needed for DDL operations performed in coordinator
            getIcebergNessieFilePathSanitizer());
    if (isRemove) {
      List<String> propertyNameList = new ArrayList<>(tableProperties.keySet());
      icebergModel.removeTableProperties(
          icebergModel.getTableIdentifier(getRootLocation()), propertyNameList);
      logger.debug(
          "Remove Table Properties in {} at version {}",
          tableSchemaComponentsWithoutPluginName,
          version);
    } else {
      icebergModel.updateTableProperties(
          icebergModel.getTableIdentifier(getRootLocation()), tableProperties);
      logger.debug(
          "Update Table Properties in {} at version {}",
          tableSchemaComponentsWithoutPluginName,
          version);
    }
  }

  protected List<String> schemaComponentsWithoutPluginName(NamespaceKey tableSchemaPath) {
    Preconditions.checkArgument(tableSchemaPath.hasParent());
    Preconditions.checkArgument(name.equalsIgnoreCase(tableSchemaPath.getRoot()));
    return tableSchemaPath.getPathWithoutRoot();
  }

  @Override
  protected FileSystem newFileSystem(String userName, OperatorContext operatorContext)
      throws IOException {
    return super.newFileSystem(userName, operatorContext);
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
    final List<String> basePath =
        new ArrayList<>(PathUtils.toPathComponents(pluginConfig.getRootPath()));
    final List<String> fullPath =
        Stream.concat(
                basePath.stream(), tableSchemaPath.stream().skip(1).map(PathUtils::removeQuotes))
            .collect(Collectors.toList());

    PathUtils.verifyNoDirectoryTraversal(
        fullPath,
        () ->
            UserException.permissionError()
                .message("Not allowed to perform directory traversal")
                .addContext("Path", fullPath.toString())
                .buildSilently());
    PathUtils.verifyNoAccessOutsideBase(PathUtils.toFSPath(basePath), PathUtils.toFSPath(fullPath));

    return fullPath;
  }

  @Override
  public boolean isIcebergMetadataValid(DatasetConfig config, NamespaceKey key) {
    return true;
  }

  @Override
  public AbstractRefreshPlanBuilder createRefreshDatasetPlanBuilder(
      SqlHandlerConfig config,
      SqlRefreshDataset sqlRefreshDataset,
      UnlimitedSplitsMetadataProvider metadataProvider,
      boolean isFullRefresh) {
    throw new UnsupportedOperationException("Metadata refresh is not supported");
  }

  @Override
  public IcebergModel getIcebergModel(
      IcebergTableProps icebergTableProps,
      String userName,
      OperatorContext operatorContext,
      FileIO fileIO,
      String userId) {
    List<String> tableKeyAsList = PathUtils.parseFullPath(icebergTableProps.getTableName());
    ResolvedVersionContext version = icebergTableProps.getVersion();
    return getIcebergModelHelper(
        tableKeyAsList, version, operatorContext, userName, userId, fileIO);
  }

  private IcebergModel getIcebergModelHelper(
      List<String> tableKeyAsList,
      ResolvedVersionContext version,
      OperatorContext operatorContext,
      String userName,
      String userId,
      FileIO fileIO) {
    Preconditions.checkNotNull(tableKeyAsList);
    Preconditions.checkNotNull(version);
    Preconditions.checkNotNull(getNessieClient());
    return new IcebergNessieVersionedModel(
        tableKeyAsList,
        getFsConfCopy(),
        fileIO,
        getNessieClient(),
        operatorContext,
        version,
        this,
        userName,
        userId,
        getIcebergNessieFilePathSanitizer());
  }

  @Override
  public String getTableLocation(IcebergTableProps tableProps) {
    return getRootLocation();
  }

  public void commitTableGrpcOperation(
      List<String> catalogKey,
      String metadataLocation,
      NessieTableAdapter nessieTableAdapter,
      ResolvedVersionContext resolvedVersionContext,
      String baseContentId,
      @Nullable IcebergCommitOrigin commitOrigin,
      String jobId,
      String userName) {
    getNessieClient()
        .commitTable(
            catalogKey,
            metadataLocation,
            nessieTableAdapter,
            resolvedVersionContext,
            baseContentId,
            commitOrigin,
            jobId,
            userName);
  }

  public Optional<NessieContent> getContentGrpcOperation(
      List<String> catalogKey, ResolvedVersionContext resolvedVersionContext, String jobId) {
    return getNessieClient().getContent(catalogKey, resolvedVersionContext, jobId);
  }

  private Optional<String> getMetadataLocation(
      List<String> catalogKey, ResolvedVersionContext resolvedVersionContext) {
    return getNessieClient()
        .getContent(catalogKey, resolvedVersionContext, null)
        .flatMap(NessieContent::getMetadataLocation);
  }

  @Override
  @WithSpan
  public @Nullable EntityType getType(List<String> catalogKey, ResolvedVersionContext version) {
    return getNessieClient()
        .getContent(catalogKey, version, null)
        .map(NessieContent::getEntityType)
        .orElse(null);
  }

  @Override
  @WithSpan
  public @Nullable String getContentId(List<String> catalogKey, ResolvedVersionContext version) {
    return getNessieClient()
        .getContent(catalogKey, version, null)
        .map(NessieContent::getContentId)
        .orElse(null);
  }

  @Override
  public NessieApiV2 getNessieApi() {
    return getNessieClient().getNessieApi();
  }

  @VisibleForTesting
  public NessieDataplaneCache<String, TableMetadata> getTableCache() {
    return tableLoadingCache;
  }

  @VisibleForTesting
  public NessieDataplaneCache<String, IcebergViewMetadata> getViewCache() {
    return viewLoadingCache;
  }

  private final class TableCacheLoader implements DataplaneCacheLoader<String, TableMetadata> {
    @Override
    public TableMetadata load(@NotNull String key) {
      return metrics.log("load_iceberg_table", () -> loadIcebergTableMetadata(key));
    }

    @Override
    public TableMetadata buildValueFromJson(String metadataLocation, String json) {
      return TableMetadataParser.fromJson(metadataLocation, json);
    }

    @Override
    public String convertValueToJson(TableMetadata tableMetadata) {
      return TableMetadataParser.toJson(tableMetadata);
    }
  }

  @WithSpan
  private TableMetadata loadIcebergTableMetadata(String metadataLocation) {
    logger.debug("Loading Iceberg table metadata from location {}", metadataLocation);
    final TableOperations tableOperations =
        new StaticTableOperations(metadataLocation, getFileIO());
    Preconditions.checkNotNull(tableOperations.current());
    return tableOperations.current();
  }

  @WithSpan
  private Table buildIcebergTable(TableMetadata tableMetadata, EntityPath datasetPath) {
    final TableOperations tableOperations = new StaticTableOperations(tableMetadata, getFileIO());
    Preconditions.checkNotNull(tableOperations.current());
    final BaseTable table =
        new BaseTable(
            tableOperations,
            String.join(".", datasetPath.getComponents().subList(1, datasetPath.size())));
    table.refresh();
    return table;
  }

  @WithSpan
  private Table getIcebergTable(
      EntityPath datasetPath, String metadataLocation, ResolvedVersionContext ver) {
    try {
      return metrics.log(
          "get_iceberg_table",
          () -> buildIcebergTable(tableLoadingCache.get(metadataLocation), datasetPath));
    } catch (NotFoundException nfe) {
      throw UserException.invalidMetadataError(nfe)
          .message(
              "Metadata for table [%s] is not available for the commit [%s]. The metadata files may have expired and been garbage collected based on the table history retention policy.",
              String.join(".", datasetPath.getComponents()), ver.getCommitHash().substring(0, 8))
          .addContext(
              String.format(
                  "Failed to locate metadata for table at the commit [%s]", ver.getCommitHash()))
          .build(logger);
    } catch (Exception e) {
      logger.error("Failed to build iceberg table", e);
      throw UserException.ioExceptionError(e)
          .message(
              "Failed to load the Iceberg table %s. "
                  + "The underlying metadata and/or data files may not exist, "
                  + "or you do not have permission to access them.",
              datasetPath)
          .build(logger);
    }
  }

  private final class ViewCacheLoader implements DataplaneCacheLoader<String, IcebergViewMetadata> {
    @Override
    public IcebergViewMetadata load(@NotNull String metadataLocation) {
      return metrics.log("load_iceberg_view", () -> loadIcebergView(metadataLocation));
    }

    @Override
    public IcebergViewMetadata buildValueFromJson(String metadataLocation, String json) {
      return IcebergViewMetadataUtils.fromJson(metadataLocation, json);
    }

    @Override
    public String convertValueToJson(IcebergViewMetadata value) {
      return value.toJson();
    }
  }

  @WithSpan
  private IcebergViewMetadata loadIcebergView(String metadataLocation) {
    logger.debug("Loading Iceberg view metadata from location {}", metadataLocation);
    IcebergViewOperationsBuilder icebergViewOperationsBuilder =
        IcebergViewOperationsBuilder.newViewOps().withFileIO(getFileIO());
    return icebergViewOperationsBuilder.build().refreshFromMetadataLocation(metadataLocation);
  }

  @WithSpan
  private IcebergViewMetadata getIcebergView(String metadataLocation) {
    return metrics.log("get_iceberg_view", () -> viewLoadingCache.get(metadataLocation));
  }

  @WithSpan
  private VersionedUdfMetadata getVersionedUdfMetadata(String metadataLocation) {
    return metrics.log("get_versioned_udf", () -> udfLoadingCache.get(metadataLocation));
  }

  private final class UdfCacheLoader implements DataplaneCacheLoader<String, VersionedUdfMetadata> {
    @Override
    public VersionedUdfMetadata load(String metadataLocation) throws Exception {
      return metrics.log("load_versioned_udf", () -> loadVersionedUdf(metadataLocation));
    }

    @Override
    public VersionedUdfMetadata buildValueFromJson(String metadataLocation, String json) {
      return VersionedUdfMetadataUtils.fromJson(json);
    }

    @Override
    public String convertValueToJson(VersionedUdfMetadata value) {
      return value.toJson();
    }
  }

  @WithSpan
  private VersionedUdfMetadata loadVersionedUdf(String metadataLocation) {
    logger.debug("Loading versioned udf metadata from location {}", metadataLocation);
    InputFile inputFile = getFileIO().newInputFile(metadataLocation);
    return VersionedUdfMetadataImpl.of(UdfMetadataParser.read(inputFile));
  }

  /** !! NEVER USE THIS CACHE ACROSS MULTIPLE REQUESTS !! */
  private final class NessieContentIdCache {
    // cache key does not include version or user info as we only use this during a single request
    private final LoadingCache<ImmutableList<String>, Optional<String>> cache;

    NessieContentIdCache(ResolvedVersionContext version) {
      cache = Caffeine.newBuilder().build(catalogKey -> loadNessieContentId(catalogKey, version));
    }

    Optional<String> getNessieContentId(List<String> catalogKey) {
      return cache.get(ImmutableList.copyOf(catalogKey));
    }
  }

  @WithSpan
  private Optional<String> loadNessieContentId(
      List<String> catalogKey, ResolvedVersionContext version) {
    return getNessieClient().getContent(catalogKey, version, null).map(NessieContent::getContentId);
  }

  private String determineSchemaId(
      NessieContentIdCache contentIdCache, ExternalNamespaceEntry entry) {
    List<String> parentCatalogKey = entry.getNamespace();
    if (parentCatalogKey.isEmpty()) {
      return "";
    }
    Optional<String> schemaId = contentIdCache.getNessieContentId(parentCatalogKey);
    if (schemaId.isEmpty()) {
      logger.warn("Failed to retrieve schema information for entry: " + entry.getNameElements());
    }
    return schemaId.orElse("");
  }

  private Optional<DataplaneViewInfo> dataplaneViewInfoRetriever(
      ExternalNamespaceEntry entry, Function<ExternalNamespaceEntry, String> schemaIdResolver) {
    // This can only return null if we forgot to request the content
    //noinspection OptionalAssignedToNull
    if (entry.getNessieContent() == null) {
      throw new IllegalStateException("dataplaneViewInfoRetriever did not request content!");
    }
    if (entry.getNessieContent().isEmpty()) {
      logger.error("dataplaneViewInfoRetriever skipping entry without content: " + entry);
      return Optional.empty();
    }
    NessieContent nessieContent = entry.getNessieContent().get();
    String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      logger.error("dataplaneViewInfoRetriever skipping entry without metadataLocation: " + entry);
      return Optional.empty();
    }
    String contentId = nessieContent.getContentId();
    try {
      EntityPath keyPath = toEntityPath(entry);

      String schemaId = schemaIdResolver.apply(entry);
      IcebergViewMetadata icebergViewMetadata = getIcebergView(metadataLocation);

      return Optional.of(
          new DataplaneViewInfo.newBuilder()
              .viewId(contentId)
              .spaceId(this.getId().getConfig().getId().getId())
              .viewName(entry.getName())
              .schemaId(schemaId)
              .path(keyPath.toString())
              .tag(getUUIDFromMetadataLocation(metadataLocation))
              .createdAt(getViewCreatedAt(icebergViewMetadata))
              .sqlDefinition(getViewSqlDefinition(icebergViewMetadata))
              .sqlContext(getViewSqlContext(icebergViewMetadata))
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
    ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
    NessieContentIdCache contentIdCache = new NessieContentIdCache(resolvedVersionContext);
    try {
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_WITH_CONTENT,
              EnumSet.of(Type.ICEBERG_VIEW),
              null)
          .map(
              entry -> dataplaneViewInfoRetriever(entry, e -> determineSchemaId(contentIdCache, e)))
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (ReferenceNotFoundException
        | NoDefaultBranchException
        | ReferenceTypeConflictException ex) {
      logger.warn(String.format("failed to retrieve all view information for source:%s", name), ex);
      return Stream.empty();
    }
  }

  private static long getViewCreatedAt(IcebergViewMetadata icebergViewMetadata) {
    if (icebergViewMetadata != null) {
      return icebergViewMetadata.getCreatedAt();
    }
    return 0L;
  }

  private static String getViewSqlDefinition(IcebergViewMetadata icebergViewMetadata) {
    if (icebergViewMetadata != null) {
      return icebergViewMetadata.getSql();
    }
    return "";
  }

  private static String getViewSqlContext(IcebergViewMetadata icebergViewMetadata) {
    if (icebergViewMetadata != null) {
      return icebergViewMetadata.getSchemaPath().toString();
    }
    return "";
  }

  private Optional<DataplaneTableInfo> dataplaneTableInfoRetriever(
      ExternalNamespaceEntry entry,
      ResolvedVersionContext resolvedVersionContext,
      Function<ExternalNamespaceEntry, String> schemaIdResolver) {
    // This can only return null if we forgot to request the content
    //noinspection OptionalAssignedToNull
    if (entry.getNessieContent() == null) {
      throw new IllegalStateException("dataplaneTableInfoRetriever did not request content!");
    }
    if (entry.getNessieContent().isEmpty()) {
      logger.error("dataplaneTableInfoRetriever skipping entry without content: " + entry);
      return Optional.empty();
    }
    NessieContent nessieContent = entry.getNessieContent().get();
    String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      logger.error("dataplaneTableInfoRetriever skipping entry without metadataLocation: " + entry);
      return Optional.empty();
    }
    String tableId = nessieContent.getContentId();
    try {
      EntityPath keyPath = toEntityPath(entry);
      Table table = getIcebergTable(keyPath, metadataLocation, resolvedVersionContext);
      String schemaId = schemaIdResolver.apply(entry);

      return Optional.of(
          new DataplaneTableInfo.newBuilder()
              .tableId(tableId != null ? tableId : "")
              .sourceId(this.getId().getConfig().getId().getId())
              .name(entry.getName())
              .schema(schemaId)
              .path(keyPath.toString())
              .tag(getUUIDFromMetadataLocation(metadataLocation))
              .formatType(entry.getType())
              .createdAt(table.history().get(0).timestampMillis())
              .build());
    } catch (Exception e) {
      logger.warn("Failed to retrieve information while calling dataplaneTableInfoRetriever", e);
      // There is no way to propagate an error in SYS.TABLES queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  @Override
  @WithSpan
  public Stream<DataplaneTableInfo> getAllTableInfo() {
    ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
    NessieContentIdCache contentIdCache = new NessieContentIdCache(resolvedVersionContext);
    try {
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_WITH_CONTENT,
              EnumSet.of(Type.ICEBERG_TABLE),
              null)
          .map(
              entry ->
                  dataplaneTableInfoRetriever(
                      entry, resolvedVersionContext, e -> determineSchemaId(contentIdCache, e)))
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (ReferenceNotFoundException
        | NoDefaultBranchException
        | ReferenceTypeConflictException ex) {
      logger.warn(
          String.format("failed to retrieve all table information for source:%s", name), ex);
      return Stream.empty();
    }
  }

  /**
   * @return Stream of Tables. If celFilter == null it means that we have nothing to
   *     find(SearchQuery) so call listEntriesIncludeNested with no filter If celFilter == null &&
   *     searchQuery != null then, we have something to find(SearchQuery) but we didn't get
   *     appropriate filter. so we don't call nessie If celFilter != null it means that by anyhow
   *     you got appropriate filter. So, call nessie with the filter. return Stream.empty if there's
   *     nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.Table> getAllInformationSchemaTableInfo(
      SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaTableInfo", e);
      return Stream.empty();
    }

    if (celFilter == null && searchQuery != null) {
      return Stream.empty();
    }
    try {
      ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_METADATA_ONLY,
              EnumSet.of(Type.ICEBERG_TABLE, Type.ICEBERG_VIEW),
              celFilter)
          .map(this::informationSchemaTableInfoRetriever)
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported
      // operations or etc, the query should be ignored
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaTableInfo", e);
      return Stream.empty();
    }
  }

  private Optional<com.dremio.service.catalog.Table> informationSchemaTableInfoRetriever(
      ExternalNamespaceEntry entry) {
    try {
      return Optional.of(
          com.dremio.service.catalog.Table.newBuilder()
              .setCatalogName(DEFAULT_CATALOG_NAME)
              .setSchemaName(joinPathExcludeEntryWithDots(entry))
              .setTableName(entry.getName())
              .setTableType(
                  entry.getType() == Type.ICEBERG_TABLE ? TableType.TABLE : TableType.VIEW)
              .build());
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling informationSchemaTableInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  /**
   * @return Stream of Views. If celFilter == null it means that we have nothing to
   *     find(SearchQuery) so call listEntriesIncludeNested with no filter If celFilter == null &&
   *     searchQuery != null then, we have something to find(SearchQuery) but we didn't get
   *     appropriate filter. so we don't call nessie If celFilter != null it means that by anyhow
   *     you got appropriate filter. So, call nessie with the filter. return Stream.empty if there's
   *     nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.View> getAllInformationSchemaViewInfo(
      SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaViewInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_WITH_CONTENT,
              EnumSet.of(Type.ICEBERG_VIEW),
              celFilter)
          .map(this::informationSchemaViewInfoRetriever)
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported
      // operations or etc, the query should be ignored
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaViewInfo", e);
      return Stream.empty();
    }
  }

  private Optional<com.dremio.service.catalog.View> informationSchemaViewInfoRetriever(
      ExternalNamespaceEntry entry) {
    // This can only return null if we forgot to request the content
    //noinspection OptionalAssignedToNull
    if (entry.getNessieContent() == null) {
      throw new IllegalStateException(
          "informationSchemaViewInfoRetriever did not request content!");
    }
    if (entry.getNessieContent().isEmpty()) {
      logger.error("informationSchemaViewInfoRetriever skipping entry without content: " + entry);
      return Optional.empty();
    }
    NessieContent nessieContent = entry.getNessieContent().get();
    String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      logger.error(
          "informationSchemaViewInfoRetriever skipping entry without metadataLocation: " + entry);
      return Optional.empty();
    }
    try {
      IcebergViewMetadata icebergViewMetadata = getIcebergView(metadataLocation);

      return Optional.of(
          com.dremio.service.catalog.View.newBuilder()
              .setCatalogName(DEFAULT_CATALOG_NAME)
              .setSchemaName(joinPathExcludeEntryWithDots(entry))
              .setTableName(entry.getName())
              .setViewDefinition(getViewSqlDefinition(icebergViewMetadata))
              .build());
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling informationSchemaViewInfoRetriever", e);
      // There is no way to propagate an error in INFORMATION_SCHEMA queries,
      // so we must squash the error and not return results for the table.
      return Optional.empty();
    }
  }

  /**
   * @return Stream of Columns. If celFilter == null it means that we have nothing to
   *     find(SearchQuery) so call listEntriesIncludeNested with no filter If celFilter == null &&
   *     searchQuery != null then, we have something to find(SearchQuery) but we didn't get
   *     appropriate filter. so we don't call nessie If celFilter != null it means that by anyhow
   *     you got appropriate filter. So, call nessie with the filter. return Stream.empty if there's
   *     nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.TableSchema> getAllInformationSchemaColumnInfo(
      SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, false, name);
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_WITH_CONTENT,
              EnumSet.of(Type.ICEBERG_TABLE, Type.ICEBERG_VIEW),
              celFilter)
          .map(entry -> informationSchemaColumnInfoRetriever(entry, resolvedVersionContext))
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported
      // operations or etc, the query should be ignored
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }
  }

  @Override
  @WithSpan
  public String getDefaultBranch() {
    return getNessieClient().getDefaultBranch().getRefName();
  }

  private Optional<com.dremio.service.catalog.TableSchema> informationSchemaColumnInfoRetriever(
      ExternalNamespaceEntry entry, ResolvedVersionContext resolvedVersionContext) {
    // This can only return null if we forgot to request the content
    //noinspection OptionalAssignedToNull
    if (entry.getNessieContent() == null) {
      throw new IllegalStateException(
          "informationSchemaColumnInfoRetriever did not request content!");
    }
    if (entry.getNessieContent().isEmpty()) {
      logger.error("informationSchemaColumnInfoRetriever skipping entry without content: " + entry);
      return Optional.empty();
    }
    NessieContent nessieContent = entry.getNessieContent().get();
    String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      logger.error(
          "informationSchemaColumnInfoRetriever skipping entry without metadataLocation: " + entry);
      return Optional.empty();
    }
    try {
      Schema schema;
      if (entry.getType() == Type.ICEBERG_TABLE) {
        EntityPath keyPath = toEntityPath(entry);
        Table table = getIcebergTable(keyPath, metadataLocation, resolvedVersionContext);
        schema = table.schema();
      } else if (entry.getType() == Type.ICEBERG_VIEW) {
        IcebergViewMetadata icebergViewMetadata = getIcebergView(metadataLocation);
        schema = icebergViewMetadata.getSchema();
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
      logger.warn(
          "Failed to retrieve information while calling informationSchemaColumnInfoRetriever", e);
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
   * @return Stream of Schemata. If celFilter == null it means that we have nothing to
   *     find(SearchQuery) so call listEntriesIncludeNested with no filter If celFilter == null &&
   *     searchQuery != null then, we have something to find(SearchQuery) but we didn't get
   *     appropriate filter. so we don't call nessie If celFilter != null it means that by anyhow
   *     you got appropriate filter. So, call nessie with the filter. return Stream.empty if there's
   *     nothing to return
   */
  @Override
  @WithSpan
  public Stream<com.dremio.service.catalog.Schema> getAllInformationSchemaSchemataInfo(
      SearchQuery searchQuery) {
    String celFilter;
    try {
      celFilter = getInformationSchemaFilter(searchQuery, true, name);
    } catch (Exception e) {
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaColumnInfo", e);
      return Stream.empty();
    }

    try {
      ResolvedVersionContext resolvedVersionContext = getNessieClient().getDefaultBranch();
      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_METADATA_ONLY,
              EnumSet.of(Type.FOLDER),
              celFilter)
          .map(this::informationSchemaSchemataInfoRetriever)
          .filter(Optional::isPresent)
          .map(Optional::get);
    } catch (Exception e) {
      // if we are failing while we are retrieving data from nessie; the query has unsupported
      // operations or etc, the query should be ignored
      logger.warn(
          "Failed to retrieve information while calling getAllInformationSchemaSchemataInfo", e);
      return Stream.empty();
    }
  }

  @Override
  public void close() {
    if (VM.areAssertsEnabled() && pluginClosed) {
      throw new IllegalStateException("DataplanePlugin already closed:\n" + pluginCloseStacktrace);
    }

    AutoCloseables.close(
        new RuntimeException("Error while closing DataplanePlugin."),
        getNessieClient(),
        super::close);

    pluginClosed = true;
    if (VM.areAssertsEnabled()) {
      pluginCloseStacktrace = VM.getCurrentStackTraceAsString();
    }
  }

  private Optional<com.dremio.service.catalog.Schema> informationSchemaSchemataInfoRetriever(
      ExternalNamespaceEntry entry) {
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
      logger.warn(
          "Failed to retrieve information while calling informationSchemaSchemataInfoRetriever", e);
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

  private FileIO getFileIO() {
    if (fileIO == null) {
      FileSystem fs = getSystemUserFS();
      Preconditions.checkState(
          fs != null, "Plugin must be started before accessing the DremioFileIO instance");
      fileIO = createIcebergFileIO(fs, null, null, null, null);
    }
    return fileIO;
  }

  private void validateEntityOfOtherTypeDoesNotExist(
      List<String> key, ResolvedVersionContext versionContext, EntityType type) {
    Optional<NessieContent> content = getNessieClient().getContent(key, versionContext, null);
    if (content.isPresent() && content.get().getEntityType() != type) {
      throw UserException.validationError()
          .message(
              "An Entity of type %s with given name %s already exists.",
              content.get().getEntityType().name(), key)
          .build(logger);
    }
  }

  @Override
  @WithSpan
  public String getName() {
    return name;
  }

  @Override
  @WithSpan
  public Optional<FunctionConfig> getFunction(CatalogEntityKey functionKey) {
    return metrics.log("get_function", () -> getFunctionHelper(functionKey));
  }

  private Optional<FunctionConfig> getFunctionHelper(CatalogEntityKey functionKey) {
    VersionContext versionContext = functionKey.getTableVersionContext().asVersionContext();
    final ResolvedVersionContext resolvedVersionContext = resolveVersionContext(versionContext);
    List<String> udfKey = schemaComponentsWithoutPluginName(functionKey.toNamespaceKey());
    final Optional<NessieContent> maybeNessieContent =
        getNessieClient().getContent(udfKey, resolvedVersionContext, null);
    if (maybeNessieContent.isEmpty()) {
      return Optional.empty();
    }
    NessieContent nessieContent = maybeNessieContent.get();
    final EntityType entityType = nessieContent.getEntityType();
    switch (entityType) {
      case UDF:
        break;
      default:
        return Optional.empty();
    }
    return Optional.ofNullable(
        createFunctionConfig(
            nessieContent, ResolvedVersionContext.convertToVersionContext(resolvedVersionContext)));
  }

  private FunctionConfig createFunctionConfig(
      NessieContent nessieContent, VersionContext versionContext) {
    FunctionConfig functionConfig = new FunctionConfig();
    List<String> combinedElements = new ArrayList<>();
    combinedElements.add(this.name);
    combinedElements.addAll(nessieContent.getCatalogKey());
    VersionedDatasetId versionedDatasetId =
        new VersionedDatasetId(
            combinedElements, nessieContent.getContentId(), TableVersionContext.of(versionContext));
    functionConfig.setId(new EntityId(versionedDatasetId.asString()));
    functionConfig.setName(PathUtils.constructFullPath(combinedElements));
    functionConfig.setFullPathList(combinedElements);
    final String metadataLocation = nessieContent.getMetadataLocation().orElse(null);
    if (metadataLocation == null) {
      logger.debug("No metadata found at {} for key {}.", metadataLocation, combinedElements);
      return null;
    }

    try {
      return populateFunctionConfigWithUdfMetadata(
          functionConfig, getVersionedUdfMetadata(metadataLocation));
    } catch (UserException e) {
      // if the UDF metadata is not accessible using source storage credential, return null
      if (UserBitShared.DremioPBError.ErrorType.PERMISSION.equals(e.getErrorType())
          || UserBitShared.DremioPBError.ErrorType.IO_EXCEPTION.equals(e.getErrorType())) {
        logger.debug(
            "Access to metadata for key {} at {} is denied.", combinedElements, metadataLocation);
        return null;
      }
      throw e;
    }
  }

  private FunctionConfig populateFunctionConfigWithUdfMetadata(
      FunctionConfig functionConfig, VersionedUdfMetadata versionedUdfMetadata) {
    functionConfig.setLastModified(versionedUdfMetadata.getLastModifiedAt());
    functionConfig.setCreatedAt(versionedUdfMetadata.getCreatedAt());
    final SchemaConverter schemaConverter =
        SchemaConverter.getBuilder()
            .setMapTypeEnabled(
                context.getOptionManager().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
            .setTableName(String.join(".", functionConfig.getFullPathList()))
            .build();

    CompleteType completeType =
        schemaConverter.fromIcebergType(versionedUdfMetadata.getReturnType());
    ReturnType returnType =
        new ReturnType()
            .setRawDataType(io.protostuff.ByteString.copyFrom(completeType.serialize()));
    functionConfig.setReturnType(returnType);
    List<FunctionArg> functionArgs =
        versionedUdfMetadata.getParameters().stream()
            .map(fa -> convertToFunctionArg(schemaConverter, fa))
            .collect(Collectors.toList());
    FunctionBody functionBody = new FunctionBody().setRawBody(versionedUdfMetadata.getBody());
    functionConfig.setFunctionDefinitionsList(
        ImmutableList.of(
            new FunctionDefinition()
                .setFunctionArgList(functionArgs)
                .setFunctionBody(functionBody)));
    return functionConfig;
  }

  private FunctionArg convertToFunctionArg(
      SchemaConverter schemaConverter, Types.NestedField nestedField) {
    return new FunctionArg()
        .setName(nestedField.name())
        .setDefaultExpression(FunctionArg.getDefaultInstance().getDefaultExpression())
        .setRawDataType(
            io.protostuff.ByteString.copyFrom(
                schemaConverter.fromIcebergType(nestedField.type()).serialize()));
  }

  @Override
  @WithSpan
  public List<FunctionConfig> getFunctions(VersionContext versionContext) {
    return metrics.log("get_functions", () -> getFunctionsHelper(versionContext));
  }

  private List<FunctionConfig> getFunctionsHelper(VersionContext versionContext) {
    try {
      ResolvedVersionContext resolvedVersionContext =
          getNessieClient().resolveVersionContext(versionContext);

      return getNessieClient()
          .listEntries(
              null,
              resolvedVersionContext,
              NestingMode.INCLUDE_NESTED_CHILDREN,
              ContentMode.ENTRY_WITH_CONTENT,
              EnumSet.of(Type.UDF),
              null)
          .map(
              entry ->
                  entry
                      .getNessieContent()
                      .map(nessieContent -> createFunctionConfig(nessieContent, versionContext)))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .collect(Collectors.toList());
    } catch (ReferenceNotFoundException
        | NoDefaultBranchException
        | ReferenceTypeConflictException ex) {
      logger.warn(
          String.format("failed to retrieve all table information for source:%s", name), ex);
      return Collections.emptyList();
    } catch (NessieForbiddenException e) {
      // If the user cannot access the specified version context, we simply return an empty list.
      logger.debug(String.format("Unable to access functions for %s.", versionContext), e);
      return Collections.emptyList();
    }
  }

  public Optional<String> getProperty(String key) {
    return getProperties().stream().filter(p -> p.name.equals(key)).map(p -> p.value).findAny();
  }

  @Override
  public String getConnection() {
    if (context.getOptionManager().getOption(DATAPLANE_LOCAL_FILE_SYSTEM_ENABLED)) {
      return "file:///";
    }
    return pluginConfig.getConnection();
  }

  @Override
  @WithSpan
  public boolean createFunction(
      CatalogEntityKey key, SchemaConfig schemaConfig, UserDefinedFunction userDefinedFunction) {
    return metrics.log(
        "create_function",
        () -> createOrUpdateFunctionHelper(key, schemaConfig, userDefinedFunction, false));
  }

  @Override
  @WithSpan
  public boolean updateFunction(
      CatalogEntityKey key, SchemaConfig schemaConfig, UserDefinedFunction userDefinedFunction) {
    return metrics.log(
        "update_function",
        () -> createOrUpdateFunctionHelper(key, schemaConfig, userDefinedFunction, true));
  }

  private boolean createOrUpdateFunctionHelper(
      CatalogEntityKey key,
      SchemaConfig schemaConfig,
      UserDefinedFunction userDefinedFunction,
      boolean isReplace) {
    Preconditions.checkNotNull(key.getTableVersionContext());
    if (key.getTableVersionContext().asVersionContext().isSpecified()
        && !key.getTableVersionContext().asVersionContext().isBranch()) {
      throw UserException.validationError()
          .message("Cannot create a function on a tag or bareCommit")
          .buildSilently();
    }

    List<String> udfKey = schemaComponentsWithoutPluginName(key.toNamespaceKey());
    ResolvedVersionContext versionContext =
        resolveVersionContext(key.getTableVersionContext().asVersionContext());

    validateEntityOfOtherTypeDoesNotExist(udfKey, versionContext, EntityType.UDF);

    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(getRootLocation());
    String warehouseLocation =
        IcebergUtils.getValidIcebergPath(path, getFsConfCopy(), getSystemUserFS().getScheme());

    List<String> schemaPath =
        schemaConfig.getDefaultSchema() != null
            ? schemaConfig.getDefaultSchema().getPathComponents()
            : Lists.newArrayList();
    SchemaConverter converter = SchemaConverter.getBuilder().build();
    List<Types.NestedField> parameters =
        converter.toIcebergNestedFields(userDefinedFunction.getFunctionArgsList());
    org.apache.iceberg.types.Type returnType =
        converter.toIcebergType(
            userDefinedFunction.getReturnType(), null, new FieldIdBroker.UnboundedFieldIdBroker());
    UdfSignature signature =
        ImmutableUdfSignature.builder()
            .signatureId(UdfUtil.generateUUID())
            .addParameters(parameters.toArray(new Types.NestedField[parameters.size()]))
            .returnType(returnType)
            .deterministic(false)
            .build();
    VersionedUdfBuilder udfBuilder =
        new VersionedUdfBuilder(udfKey)
            .withVersionContext(versionContext)
            .withWarehouseLocation(warehouseLocation)
            .withNessieClient(getNessieClient())
            .withFileIO(getFileIO())
            .withUserName(schemaConfig.getUserName())
            .withDefaultNamespace(Namespace.of(schemaPath.toArray(new String[schemaPath.size()])))
            .withSignature(signature)
            .withBody(
                VersionedUdfMetadata.SupportedUdfDialects.DREMIOSQL.toString(),
                userDefinedFunction.getFunctionSql(),
                null);

    Udf udf = isReplace ? udfBuilder.createOrReplace() : udfBuilder.create();

    return true;
  }

  @Override
  @WithSpan
  public void dropFunction(CatalogEntityKey key, SchemaConfig schemaConfig) {
    metrics.log("drop_function", () -> dropFunctionHelper(key, schemaConfig));
  }

  private void dropFunctionHelper(CatalogEntityKey key, SchemaConfig schemaConfig) {
    Preconditions.checkNotNull(key.getTableVersionContext());
    VersionContext versionContext = key.getTableVersionContext().asVersionContext();
    final ResolvedVersionContext resolvedVersionContext = resolveVersionContext(versionContext);

    logger.debug("Dropping function '{}'", key);
    List<String> udfKey = schemaComponentsWithoutPluginName(key.toNamespaceKey());
    getNessieClient()
        .deleteCatalogEntry(
            udfKey, EntityType.UDF, resolvedVersionContext, schemaConfig.getUserName());
  }

  boolean folderExists(List<String> folderKey, ResolvedVersionContext resolvedVersionContext) {
    if (folderKey == null || folderKey.isEmpty()) {
      return false;
    }
    EntityType type = getType(folderKey, resolvedVersionContext);
    if (type == EntityType.FOLDER) {
      return true;
    }
    return false;
  }

  public IcebergNessieFilePathSanitizer getIcebergNessieFilePathSanitizer() {
    switch (pluginConfig.getStorageProvider()) {
      case AZURE:
        return new AzureStorageIcebergFilePathSanitizer();
      case AWS:
        return s -> s; // AWS supports special characters, so no need to sanitize
      case GOOGLE:
        return s -> s; // GCS supports a similar set as AWS
      default:
        throw new IllegalStateException("Unexpected value: " + pluginConfig.getStorageProvider());
    }
  }

  private String addUniqueSuffix() {
    return "_" + UUID.randomUUID();
  }

  protected void validateStorageProviderTypeEnabled(OptionManager optionManager) {
    final BooleanValidator option;
    final String storageProviderName;

    switch (pluginConfig.getStorageProvider()) {
      case AWS:
        option = DATAPLANE_AWS_STORAGE_ENABLED;
        storageProviderName = "AWS";
        break;
      case AZURE:
        option = DATAPLANE_AZURE_STORAGE_ENABLED;
        storageProviderName = "Azure";
        break;
      case GOOGLE:
        option = DATAPLANE_GCS_STORAGE_ENABLED;
        storageProviderName = "Google";
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + pluginConfig.getStorageProvider());
    }

    if (!optionManager.getOption(option)) {
      throw UserException.validationError()
          .message(String.format("%s storage provider type is not supported.", storageProviderName))
          .build(logger);
    }
  }

  @Override
  public SourceState getState() {
    return getState(getNessieClient(), name, context);
  }

  /**
   * Get the Nessie commit user id from the RequestContext. Only used/needed by LakehouseCatalog
   * plugin. Return null for other plugins
   *
   * @return
   */
  protected String getNessieCommitUserId() {
    return null;
  }

  public abstract SourceState getState(
      NessieClient nessieClient, String name, SabotContext context);

  public abstract void validatePluginEnabled(SabotContext context);

  public abstract void validateConnectionToNessieRepository(
      NessieClient nessieClient, String name, SabotContext context);

  public abstract void validateNessieSpecificationVersion(NessieClient nessieClient);

  public void validateRootPath() {
    switch (pluginConfig.getStorageProvider()) {
      case AWS:
        if (!CloudStoragePathValidator.isValidAwsS3RootPath(pluginConfig.getRootPath())) {
          throw UserException.validationError()
              .message(
                  "Failure creating or updating %s source. Invalid AWS S3 root path. You must provide a valid AWS S3 path. Example: /bucket-name/path",
                  pluginConfig.getSourceTypeName())
              .build(logger);
        }
        break;
      case AZURE:
        if (!CloudStoragePathValidator.isValidAzureStorageRootPath(pluginConfig.getRootPath())) {
          throw UserException.validationError()
              .message(
                  "Failure creating or updating %s source. Invalid Azure root path. You must provide a valid Azure root path. Example: /containerName/path",
                  pluginConfig.getSourceTypeName())
              .build(logger);
        }
        break;
      case GOOGLE:
        if (!CloudStoragePathValidator.isValidGcsRootPath(pluginConfig.getRootPath())) {
          throw UserException.validationError()
              .message(
                  "Failure creating or updating %s source. Invalid GCS root path. You must provide a valid GCS root path. Example: /bucket-name/path",
                  pluginConfig.getSourceTypeName())
              .build(logger);
        }
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + pluginConfig.getStorageProvider());
    }
  }

  @Override
  public void start() throws IOException {
    this.validatePluginEnabled(context);
    this.validateStorageProviderTypeEnabled(context.getOptionManager());
    this.validateRootPath();
    try {
      this.validateNessieSpecificationVersion(getNessieClient());
    } catch (Exception e) {
      throw UserException.validationError(e)
          .message("Unable to create source [%s], %s", name, e.getMessage())
          .buildSilently();
    }
    try {
      this.validateConnectionToNessieRepository(getNessieClient(), name, context);
    } catch (Exception e) {
      throw UserException.resourceError(e)
          .message("Unable to create source [%s], %s", name, e.getMessage())
          .buildSilently();
    }
    super.start();
  }
}
