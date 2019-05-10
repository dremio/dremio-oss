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
package com.dremio.service.reflection.materialization;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import javax.inject.Provider;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

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
import com.dremio.datastore.KVStoreProvider;
import com.dremio.exec.catalog.CurrentSchemaOption;
import com.dremio.exec.catalog.FileConfigOption;
import com.dremio.exec.catalog.SortColumnsOption;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.logical.ViewTable;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.exec.store.dfs.FileDatasetHandle;
import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.PreviousDatasetInfo;
import com.dremio.exec.store.file.proto.FileProtobuf.FileUpdateKey;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatDatasetAccessor;
import com.dremio.exec.store.parquet.ParquetFormatPlugin;
import com.dremio.service.DirectProvider;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.dremio.service.reflection.ReflectionServiceImpl;
import com.dremio.service.reflection.proto.Materialization;
import com.dremio.service.reflection.proto.MaterializationId;
import com.dremio.service.reflection.proto.MaterializationState;
import com.dremio.service.reflection.proto.ReflectionId;
import com.dremio.service.reflection.proto.Refresh;
import com.dremio.service.reflection.store.MaterializationStore;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * A custom FileSystemPlugin that only works with Parquet files and generates file selections based on Refreshes as opposed to path.
 */
public class AccelerationStoragePlugin extends FileSystemPlugin<AccelerationStoragePluginConfig> {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(AccelerationStoragePlugin.class);

  private static final FileUpdateKey EMPTY = FileUpdateKey.getDefaultInstance();
  private MaterializationStore materializationStore;
  private ParquetFormatPlugin formatPlugin;

  public AccelerationStoragePlugin(AccelerationStoragePluginConfig config, SabotContext context, String name, Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
  }

  @Override
  public void start() throws IOException {
    super.start();
    materializationStore = new MaterializationStore(DirectProvider.<KVStoreProvider>wrap(getContext().getKVStoreProvider()));
    formatPlugin = (ParquetFormatPlugin) formatCreator.getFormatPluginByConfig(new ParquetFormatConfig());
  }

  @Override
  public ViewTable getView(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    return null;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return getContext().getConfig().getClass("dremio.plugins.acceleration.rulesfactory",
        StoragePluginRulesFactory.class,
        super.getRulesFactoryClass());
  }
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
   * @param components
   *          The path components. First item is expected to be the Accelerator storage plugin, then
   *          we expect either two more parts: ReflectionId and MaterializationId or a single two
   *          part slashed value of ReflectionId/MaterializationId.
   * @return List of refreshes or null if there are no matching refreshes.
   */
  private FluentIterable<Refresh> getSlices(List<String> components) {
    components = normalizeComponents(components);
    if (components == null) {
      return null;
    }

    ReflectionId reflectionId = new ReflectionId(components.get(1));
    MaterializationId id = new MaterializationId(components.get(2));
    Materialization materialization = materializationStore.get(id);

    if(materialization == null) {
      logger.info("Unable to find materialization id: {}", id.getId());
      return null;
    }

    // verify that the materialization has the provided reflection.
    if(!materialization.getReflectionId().equals(reflectionId)) {
      logger.info("Mismatched reflection id for materialization. Expected: {}, Actual: {}, for MaterializationId: {}", reflectionId.getId(), materialization.getReflectionId().getId(), id.getId());
      return null;
    }

    FluentIterable<Refresh> refreshes = materializationStore.getRefreshes(materialization);

    if(refreshes.isEmpty()) {
      logger.info("No slices for materialization MaterializationId: {}", id.getId());
      return null;
    }

    return refreshes;
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath datasetPath, GetDatasetOption... options) throws ConnectorException {
    FluentIterable<Refresh> refreshes = getSlices(datasetPath.getComponents());
    if(refreshes == null) {
      return Optional.empty();
    }

    final String selectionRoot = new Path(getConfig().getPath(), refreshes.first().get().getReflectionId().getId()).toString();

    ImmutableList<FileStatus> allStatus = refreshes.transformAndConcat((Function<Refresh, Iterable<FileStatus>>) input -> {
      try {
        FileSelection selection = FileSelection.create(getSystemUserFS(), resolveTablePathToValidPath(input.getPath()));
        if(selection != null) {
          return selection.minusDirectories().getFileStatuses();
        }
        throw new IllegalStateException("Unable to retrieve selection for path." + input.getPath());
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }).toList();

    BatchSchema currentSchema = CurrentSchemaOption.getSchema(options);
    FileConfig fileConfig = FileConfigOption.getFileConfig(options);
    List<String> sortColumns = SortColumnsOption.getSortColumns(options);
    Integer fieldCount = MaxLeafFieldCount.getCount(options);

    FileSelection selection = FileSelection.createFromExpanded(allStatus, selectionRoot);

    final PreviousDatasetInfo pdi = new PreviousDatasetInfo(fileConfig, currentSchema, sortColumns);
    return Optional.of(new ParquetFormatDatasetAccessor(DatasetType.PHYSICAL_DATASET_SOURCE_FOLDER, getSystemUserFS(), selection,
        this, new NamespaceKey(datasetPath.getComponents()), EMPTY, formatPlugin, pdi, 800 /* TODO */));
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

  @Override
  public void dropTable(List<String> tableSchemaPath, SchemaConfig schemaConfig) {
    final List<String> components = normalizeComponents(tableSchemaPath);
    if (components == null) {
      throw UserException.validationError().message("Unable to find any materialization or associated refreshes.").build(logger);
    }

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
    Iterable<Refresh> refreshes = materializationStore.getRefreshesExclusivelyOwnedBy(materialization);
    if (Iterables.isEmpty(refreshes)) {
      logger.debug("deleted materialization {} has no associated refresh");
      return;
    }

    for (Refresh r : refreshes) {
      try {
        //TODO once DX-10850 is fixed we should no longer need to split the refresh path into separate components
        final List<String> tableSchemaPath = ImmutableList.<String>builder()
          .add(ReflectionServiceImpl.ACCELERATOR_STORAGEPLUGIN_NAME)
          .addAll(PathUtils.toPathComponents(r.getPath()))
          .build();
        logger.debug("deleting refresh {}", tableSchemaPath);
        super.dropTable(tableSchemaPath, schemaConfig);
      } catch (Exception e) {
        logger.warn("Couldn't delete refresh {}", r.getId().getId(), e);
      } finally {
        materializationStore.delete(r.getId());
      }
    }
  }
}
