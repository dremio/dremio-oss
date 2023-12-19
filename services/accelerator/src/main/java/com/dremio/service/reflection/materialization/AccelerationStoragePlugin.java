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
package com.dremio.service.reflection.materialization;

import static com.dremio.exec.ExecConstants.ICEBERG_CATALOG_TYPE_KEY;
import static com.dremio.exec.ExecConstants.ICEBERG_NAMESPACE_KEY;
import static com.dremio.service.reflection.ReflectionOptions.NESSIE_REFLECTIONS_NAMESPACE;
import static com.dremio.service.reflection.ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.inject.Provider;

import org.apache.iceberg.Table;

import com.dremio.common.FSConstants;
import com.dremio.common.SuppressForbidden;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.utils.PathUtils;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.ValidateMetadataOption;
import com.dremio.connector.metadata.options.MaxLeafFieldCount;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.TableMutationOptions;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.dfs.MayBeDistFileSystemPlugin;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.iceberg.IcebergExecutionDatasetAccessor;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatPlugin;
import com.dremio.exec.store.iceberg.TableSchemaProvider;
import com.dremio.exec.store.iceberg.TableSnapshotProvider;
import com.dremio.exec.store.iceberg.TimeTravelProcessors;
import com.dremio.exec.store.iceberg.model.IcebergCatalogType;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergTableLoader;
import com.dremio.exec.store.metadatarefresh.MetadataRefreshUtils;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.io.file.FileAttributes;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.capabilities.BooleanCapabilityValue;
import com.dremio.service.namespace.capabilities.CapabilityValue;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * A custom FileSystemPlugin that generates file selections based on Refreshes as opposed to path.
 *
 * A reflection's materialization has both a logical and physical path.  The logical path
 * is referenced in the query trees and consists of a reflection_id and a materialization_id corresponding to
 * the most recent refresh or optimize job.
 *
 * Every logical path will map to a physical Iceberg table by joining from Materialization to Refreshes
 * on series_id and series_ordinal and looking up the physical location from Refreshes.basePath.
 *
 * The same reflection could have a new series_id when the reflection schema changes (and a full refresh ensues).
 * The same series_id will have a new series_ordinal when an incremental refresh results in a data change.
 *
 * For incremental refreshes, if there is no data or schema change, the series_id and series_ordinal will not change.
 * However, there will always be a new Materialization (hence new logical path) to track the refresh and updated expiry.
 *
 */
public class AccelerationStoragePlugin extends MayBeDistFileSystemPlugin<AccelerationStoragePluginConfig> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationStoragePlugin.class);
  public static final int TABLE_SCHEMA_PATH_REFLECTION_ID_COMPONENT = 1;
  private static final int TABLE_SCHEMA_PATH_MATERIALIZATION_ID_COMPONENT = 2;
  public static final BooleanCapabilityValue CAN_USE_PARTITION_STATS = new BooleanCapabilityValue(SourceCapabilities.getCanUsePartitionStats(), true);
  private static final FileUpdateKey EMPTY = FileUpdateKey.getDefaultInstance();
  private MaterializationStore materializationStore;
  private ParquetFormatPlugin parquetFormatPlugin;
  private IcebergFormatPlugin icebergFormatPlugin;

  public AccelerationStoragePlugin(AccelerationStoragePluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  // Constructor exclusively for AccelerationStoragePluginTests
  protected AccelerationStoragePlugin(AccelerationStoragePluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider, MaterializationStore matStore) {
    super(config, context, name, idProvider);
    materializationStore = matStore;
  }

  @Override
  protected List<Property> getProperties() {
    List<Property> props = new ArrayList<>(super.getProperties());
    props.add(new Property(FSConstants.FS_S3A_FILE_STATUS_CHECK, Boolean.toString(getConfig().isS3FileStatusCheckEnabled())));
    props.add(new Property(ICEBERG_CATALOG_TYPE_KEY, IcebergCatalogType.NESSIE.name()));
    props.add(new Property(ICEBERG_NAMESPACE_KEY,
            getContext().getOptionManager().getOption(NESSIE_REFLECTIONS_NAMESPACE)));
    return props;
  }

  @Override
  public void start() throws IOException {
    super.start();
    materializationStore = new MaterializationStore(DirectProvider.<LegacyKVStoreProvider>wrap(getContext().getKVStoreProvider()));
    parquetFormatPlugin = (ParquetFormatPlugin) formatCreator.getFormatPluginByConfig(new ParquetFormatConfig());
    icebergFormatPlugin = (IcebergFormatPlugin)formatCreator.getFormatPluginByConfig(new IcebergFormatConfig());
  }

  @Override
  public FileSystem createFS(String userName, OperatorContext operatorContext, boolean metadata) throws IOException {
    FileSystem fs = new AccelerationFileSystem(super.createFS(userName, operatorContext, metadata));
    if (fs.isPdfs()) {
      // Logging to help with debugging DX-54664
      IllegalStateException exception = new IllegalStateException("AccelerationStoragePlugin does not support PDFS.  User: " + userName);
      logger.error(exception.getMessage(), exception);
    }
    return fs;
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  /**
   *
   * @param components
   *    The path components. First item is expected to be the Accelerator storage plugin, then
   *    we expect either two more parts: ReflectionId and MaterializationId or a single two
   *    part slashed value of ReflectionId/MaterializationId.
   * @return List with three entries or null
   */
  private List<String> normalizeComponents(final List<String> components) {
    if (components.size() != 2 && components.size() != 3) {
      return null;
    }

    if (components.size() == 3) {
      return components;
    }

    // there are two components, let's see if we can split them up (using only slash paths instead of dotted paths).
    final String[] pieces = components.get(1).split("/");
    if(pieces.length != 2) {
      return null;
    }

    return ImmutableList.of(components.get(0), pieces[0], pieces[1]);
  }

  /**
   * Find the set of refreshes/slices associated with a particular materialization. Could be one to
   * many. If no refreshes are found, the materialization cannot be served.
   *
   * @return List of refreshes or null if there are no matching refreshes.
   */
  private FluentIterable<Refresh> getSlices(Materialization materialization, ReflectionId reflectionId) {
    // verify that the materialization has the provided reflection.
    if(!materialization.getReflectionId().equals(reflectionId)) {
      logger.info("Mismatched reflection id for materialization. Expected: {}, Actual: {}, for MaterializationId: {}",
        reflectionId.getId(), materialization.getReflectionId().getId(), materialization.getId().getId());
      return null;
    }

    FluentIterable<Refresh> refreshes = materializationStore.getRefreshes(materialization);

    if(refreshes.isEmpty()) {
      logger.info("No slices for materialization MaterializationId: {}", materialization.getId().getId());
      return null;
    }

    return refreshes;
  }

  private Materialization getMaterialization(MaterializationId id) {
    Materialization materialization = materializationStore.get(id);

    if (materialization == null) {
      logger.info("Unable to find materialization id: {}", id.getId());
    }

    return materialization;
  }

  @SuppressForbidden // guava Optional
  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    List<String> components = normalizeComponents(datasetPath.getComponents());
    if (components == null) {
      return Optional.empty();
    }
    Preconditions.checkState(components.size() == 3, "Unexpected number of components in path");

    ReflectionId reflectionId = new ReflectionId(components.get(1));
    MaterializationId materializationId = new MaterializationId(components.get(2));
    Materialization materialization = getMaterialization(materializationId);
    if (materialization == null) {
      return Optional.empty();
    }

    FluentIterable<Refresh> refreshes = getSlices(materialization, reflectionId);
    if(refreshes == null) {
      return Optional.empty();
    }

    final String selectionRoot = getConfig().getPath().resolve(refreshes.first().get().getReflectionId().getId()).toString();

    BatchSchema currentSchema = CurrentSchemaOption.getSchema(options);
    FileConfig fileConfig = FileConfigOption.getFileConfig(options);
    List<String> sortColumns = SortColumnsOption.getSortColumns(options);
    Integer fieldCount = MaxLeafFieldCount.getCount(options);

    boolean icebergDataset = isUsingIcebergDataset(materialization);
    final FileSelection selection = getFileSelection(datasetPath.getName(), refreshes, selectionRoot, icebergDataset);

    final PreviousDatasetInfo pdi = new PreviousDatasetInfo(fileConfig, currentSchema, sortColumns, null, null, true);
    if (!icebergDataset) {
      FileDatasetHandle.checkMaxFiles(datasetPath.getName(), selection.getFileAttributesList().size(), parquetFormatPlugin.getMaxFilesLimit());
    }
    return getDatasetHandle(datasetPath, fieldCount, icebergDataset, selection, pdi);
  }

  private Boolean isUsingIcebergDataset(Materialization materialization) {
    Boolean isIcebergDataset = materialization.getIsIcebergDataset();
    return isIcebergDataset != null && isIcebergDataset;
  }

  private Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, Integer fieldCount, boolean icebergDataset, FileSelection selection, PreviousDatasetInfo pdi) {
    if (icebergDataset) {
      final Supplier<Table> tableSupplier = Suppliers.memoize(
          () -> {
            final IcebergModel icebergModel = getIcebergModel();
            final IcebergTableLoader icebergTableLoader = icebergModel.getIcebergTableLoader(
                icebergModel.getTableIdentifier(selection.getSelectionRoot()));
            return icebergTableLoader.getIcebergTable();
          }
      );

      // TODO: create a DX!
      final TableSnapshotProvider tableSnapshotProvider =
          TimeTravelProcessors.getTableSnapshotProvider(null, null);
      final TableSchemaProvider tableSchemaProvider =
              TimeTravelProcessors.getTableSchemaProvider(null);
      return Optional.of(new IcebergExecutionDatasetAccessor(datasetPath, tableSupplier, getFsConfCopy(),
          icebergFormatPlugin, getSystemUserFS(), tableSnapshotProvider, this, tableSchemaProvider));
    } else {
      return Optional.of(new ParquetFormatDatasetAccessor(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, getSystemUserFS(), selection,
        this, new NamespaceKey(datasetPath.getComponents()), EMPTY, parquetFormatPlugin, pdi, fieldCount, getContext()));
    }
  }

  private FileSelection getFileSelection(String datasetName, FluentIterable<Refresh> refreshes, String selectionRoot, boolean icebergDataset) {
    return icebergDataset ? getIcebergFileSelection(refreshes) : getParquetFileSelection(datasetName, refreshes, selectionRoot);
  }

  private FileSelection getParquetFileSelection(String datasetName, FluentIterable<Refresh> refreshes, String selectionRoot) {
    FileSelection selection;
    List<Refresh> refreshList = refreshes.toList();
    List<FileSelection> fileSelections = refreshList.stream()
      .map(refresh -> {
        try {
          FileSelection currentRefreshSelection = FileSelection.create(datasetName, getSystemUserFS(), resolveTablePathToValidPath(refresh.getPath()), parquetFormatPlugin.getMaxFilesLimit());
          if (currentRefreshSelection != null) {
            return currentRefreshSelection;
          }
          throw new IllegalStateException("Unable to retrieve selection for path." + refresh.getPath());
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      })
      .collect(Collectors.toList());

    ImmutableList<FileAttributes> allStatus = fileSelections.stream()
      .flatMap(fileSelection -> {
        try {
          return fileSelection.minusDirectories().getFileAttributesList().stream();
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      })
      .collect(ImmutableList.toImmutableList());
    selection = FileSelection.createFromExpanded(allStatus, selectionRoot);
    return selection;
  }

  private FileSelection getIcebergFileSelection(FluentIterable<Refresh> refreshes) {
    Preconditions.checkState(refreshes.size() > 0, "Unexpected state");
    return getIcebergFileSelection(refreshes.get(refreshes.size() - 1).getPath());
  }

  public FileSelection getIcebergFileSelection(String path) {
    try {
      return FileSelection.createNotExpanded(getSystemUserFS(), resolveTablePathToValidPath(path));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public void deleteIcebergTableRootPointer(String userName, Path icebergTablePath) {
    icebergTablePath = Path.of(getConfig().getPath().toString()).resolve(icebergTablePath.toString());
    super.deleteIcebergTableRootPointer(userName, icebergTablePath);
  }

  /**
   * Converts a logical materialization path to its physical filesystem path.
   */
  @Override
  protected Path getPath(NamespaceKey table, String userName) {
    Path path = null;
    try {
      final List<String> components = normalizeComponents(table.getPathComponents());
      final ReflectionId reflectionId = new ReflectionId(components.get(TABLE_SCHEMA_PATH_REFLECTION_ID_COMPONENT));
      final MaterializationId materializationId = new MaterializationId(components.get(TABLE_SCHEMA_PATH_MATERIALIZATION_ID_COMPONENT));
      final Materialization materialization = materializationStore.get(materializationId);
      Refresh refresh = materializationStore.getMostRecentRefresh(reflectionId, materialization.getSeriesId());
      final NamespaceKey tableSchemaPath = new NamespaceKey(ImmutableList.<String>builder()
        .add(ACCELERATOR_STORAGEPLUGIN_NAME)
        .addAll(PathUtils.toPathComponents(refresh.getPath()))
        .build());
      final String tableName = getTableName(tableSchemaPath);
      path = resolveTablePathToValidPath(tableName);
    } catch (RuntimeException e) {
      logger.error("Unable to get filesystem path for materialization path: {}", table, e);
    }
    return path;
  }

  @Override
  public DatasetMetadata getDatasetMetadata(
     DatasetHandle datasetHandle,
     PartitionChunkListing chunkListing,
     GetMetadataOption... options
  ) throws ConnectorException {
    return datasetHandle.unwrap(FileDatasetHandle.class).getDatasetMetadata(options);
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    List<CapabilityValue<?,?>> capabilities = super.getSourceCapabilities().getCapabilitiesList();
    List<CapabilityValue<?,?>> newCapabilities = new ArrayList() {
      {
        add(CAN_USE_PARTITION_STATS);
        addAll(capabilities);
      }
    };
    return new SourceCapabilities(newCapabilities);
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

  /**
   * The behavior of dropTable depends on whether the reflection is full or incremental.  For full, we need to clean up
   * the KV store, Nessie and Filesystem Iceberg data and metadata files.  For incremental, it gets trickier because
   * we need to see if the refection's materialization is the last one to determine whether we are dropping the whole
   * materialization or just cleaning up some prior incremental refresh.  If cleaning up a prior incremental refresh, we
   * need to only clean up the KV store.  The Iceberg table still belongs to the latest refresh and should not
   * be removed.
   *
   * @param tableSchemaPath Logical path to the reflection's materialization
   * @param schemaConfig
   * @param tableMutationOptions
   */
  @Override
  public void dropTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {

    final List<String> components = normalizeComponents(tableSchemaPath.getPathComponents());
    if (components == null) {
      throw UserException.validationError().message("Unable to find any materialization or associated refreshes.").build(logger);
    }
    Preconditions.checkState(components.size() == 3, "Unexpected number of components in path");

    final ReflectionId reflectionId = new ReflectionId(components.get(1));
    final MaterializationId materializationId = new MaterializationId(components.get(2));
    final Materialization materialization = materializationStore.get(materializationId);

    if(materialization == null) {
      throw UserException.validationError().message("Cannot delete a non existent materialization.").build(logger);
    }

    // verify that the materialization has the provided reflection.
    if(!materialization.getReflectionId().equals(reflectionId)) {
      throw UserException.validationError().message("Mismatched reflection id for materialization. Expected: %s, Actual: %s", reflectionId.getId(), materialization.getReflectionId().getId()).build(logger);
    }

    if (materialization.getState() == MaterializationState.RUNNING) {
      throw UserException.validationError().message("Cannot delete a running materialization.").build(logger);
    }

    try {
      deleteOwnedRefreshes(materialization, schemaConfig);
    } finally {
      // let's make sure we delete the entry otherwise we may keep trying to delete it over and over again
      materializationStore.delete(materialization.getId());
    }
  }

  private void deleteOwnedRefreshes(Materialization materialization, SchemaConfig schemaConfig) {
    List<Refresh> refreshes = ImmutableList.copyOf(materializationStore.getRefreshesExclusivelyOwnedBy(materialization));
    if (refreshes.isEmpty()) {
      logger.debug("deleted materialization {} has no associated refresh", materialization);
      return;
    }

    // Keep track of refreshes that we have already deleted, so we do not attempt to delete the same path repeatedly.
    Set<NamespaceKey> deletedPaths = new HashSet<>();

    for (Refresh r : refreshes) {
      try {
        //TODO once DX-10850 is fixed we should no longer need to split the refresh path into separate components
        final NamespaceKey tableSchemaPath = new NamespaceKey(ImmutableList.<String>builder()
          .add(ACCELERATOR_STORAGEPLUGIN_NAME)
          .addAll(PathUtils.toPathComponents(r.getPath()))
          .build());
        if (!deletedPaths.contains(tableSchemaPath)) {
          deletedPaths.add(tableSchemaPath);
          logger.debug("deleting refresh {}", tableSchemaPath);
          boolean isLayered = r.getIsIcebergRefresh() != null && r.getIsIcebergRefresh();
          TableMutationOptions tableMutationOptions = TableMutationOptions.newBuilder()
            .setIsLayered(isLayered)
            .setShouldDeleteCatalogEntry(isLayered).build();
          fileSystemPluginDropTable(tableSchemaPath, schemaConfig, tableMutationOptions);
        }
      } catch (Exception e) {
        logger.warn("Couldn't delete refresh {}", r.getId().getId(), e);
      } finally {
        materializationStore.delete(r.getId());
      }
    }
  }

  @Override
  protected boolean ctasToUseIceberg() {
    return MetadataRefreshUtils.unlimitedSplitsSupportEnabled(getContext().getOptionManager());
  }

  // Calls FileSystemPlugin dropTable method. Used when dropTable is called, created to allow for ease of testing.
  @VisibleForTesting
  void fileSystemPluginDropTable(NamespaceKey tableSchemaPath, SchemaConfig schemaConfig, TableMutationOptions tableMutationOptions) {
    super.dropTable(tableSchemaPath, schemaConfig, tableMutationOptions);
  }

  /**
   * Override used for OPTIMIZE TABLE and VACUUM TABLE commands on reflections. Redirects the path from the logical path to the correct physical path.
   */
  @Override
  public CreateTableEntry createNewTable(NamespaceKey tableSchemaPath, SchemaConfig config, IcebergTableProps icebergTableProps,
                                         WriterOptions writerOptions, Map<String, Object> storageOptions,
                                         boolean isResultsTable) {
    // If the source operation is OPTIMIZE or VACUUM, this is an incremental reflection. Thus, the table path needs to be corrected
    // to point towards the physical location of the materialization (series ordinal 0).
    if (icebergTableProps != null) {
      IcebergCommandType commandType = icebergTableProps.getIcebergOpType();
      if (commandType != null && (commandType.equals(IcebergCommandType.OPTIMIZE) || commandType.equals(IcebergCommandType.VACUUM))) {
        String materializationPath = materializationStore.get(new MaterializationId(tableSchemaPath.getPathComponents().get(TABLE_SCHEMA_PATH_MATERIALIZATION_ID_COMPONENT))).getBasePath();
        List<String> correctedPath = Arrays.asList(ACCELERATOR_STORAGEPLUGIN_NAME, tableSchemaPath.getPathComponents().get(TABLE_SCHEMA_PATH_REFLECTION_ID_COMPONENT), materializationPath);
        tableSchemaPath = new NamespaceKey(correctedPath);
      }
    }
    return super.createNewTable(tableSchemaPath, config, icebergTableProps, writerOptions, storageOptions, isResultsTable);
  }
}
