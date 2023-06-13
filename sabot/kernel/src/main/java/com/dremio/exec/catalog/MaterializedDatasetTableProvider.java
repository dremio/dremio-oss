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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.inject.Provider;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.VersionedDatasetHandle;
import com.dremio.options.OptionManager;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
import com.dremio.service.namespace.proto.EntityId;
import com.google.common.base.Suppliers;

/**
 * Lazily loads dataset metadata.
 */
public class MaterializedDatasetTableProvider implements Provider<MaterializedDatasetTable> {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MaterializedDatasetTableProvider.class);

  /*
   * If null, a synthetic shallow config is created before extended attributes are added.
   */
  private final DatasetConfig currentConfig;
  private final DatasetHandle handle;
  private final StoragePlugin plugin;
  private final StoragePluginId pluginId;
  private final SchemaConfig schemaConfig;
  private final DatasetRetrievalOptions options;
  private final OptionManager optionManager;

  public MaterializedDatasetTableProvider(
      DatasetConfig currentConfig,
      DatasetHandle handle,
      StoragePlugin plugin,
      StoragePluginId pluginId,
      SchemaConfig schemaConfig,
      DatasetRetrievalOptions options,
      OptionManager optionManager) {
    this.currentConfig = currentConfig;
    this.handle = handle;
    this.plugin = plugin;
    this.pluginId = pluginId;
    this.schemaConfig = schemaConfig;
    this.options = options;
    this.optionManager = optionManager;
  }

  @Override
  public MaterializedDatasetTable get() {
    final Supplier<PartitionChunkListing> partitionChunkListing = Suppliers.memoize(this::getPartitionChunkListing);

    final Supplier<List<PartitionProtobuf.PartitionChunk>> partitionChunks =
        Suppliers.memoize(() -> toPartitionChunks(partitionChunkListing));

    final Supplier<DatasetConfig> datasetConfig = Suppliers.memoize(
        () -> createDatasetConfig(partitionChunkListing, partitionChunks));

    TableVersionContext versionContext =
      options.versionedDatasetAccessOptions() != null && this.options.versionedDatasetAccessOptions().getVersionContext() != null ?
        TableVersionContext.of(this.options.versionedDatasetAccessOptions().getVersionContext()) : null;
    if (versionContext == null && options.getTimeTravelRequest() != null) {
      // Only applies to versioned tables outside of Nessie
      versionContext = TableVersionContext.of(options.getTimeTravelRequest());
    }

    return new MaterializedDatasetTable(MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath()), pluginId,
        schemaConfig.getUserName(), datasetConfig, partitionChunks,
        optionManager.getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT), versionContext);
  }

  private PartitionChunkListing getPartitionChunkListing() {
    try {
      return plugin.listPartitionChunks(handle, options.asListPartitionChunkOptions(currentConfig));
    } catch (ConnectorException e) {
      throw UserException.validationError(e).buildSilently();
    }
  }

  private List<PartitionProtobuf.PartitionChunk> toPartitionChunks(Supplier<PartitionChunkListing> listingSupplier) {
    final Iterator<? extends PartitionChunk> chunks = listingSupplier.get()
        .iterator();

    final List<PartitionProtobuf.PartitionChunk> toReturn = new ArrayList<>();
    int i = 0;
    while (chunks.hasNext()) {
      final PartitionChunk chunk = chunks.next();
      toReturn.addAll(MetadataObjectsUtils.newPartitionChunk(i + "-", chunk)
          .collect(Collectors.toList()));
      i++;
    }

    return toReturn;
  }

  private DatasetConfig createDatasetConfig(
      Supplier<PartitionChunkListing> listingSupplier,
      Supplier<List<PartitionProtobuf.PartitionChunk>> partitionChunks
  ) {
    // Ensure partition chunks are populated by calling partitionChunks.get()
    // and also calculate the record count from split information.
    final long recordCountFromSplits = partitionChunks.get().stream()
        .mapToLong(PartitionProtobuf.PartitionChunk::getRowCount)
        .sum();

    final DatasetConfig toReturn = currentConfig != null ? currentConfig :
      MetadataObjectsUtils.newShallowConfig(handle);
    if (handle instanceof VersionedDatasetHandle) {
      //AT BRANCH/TAG/COMMIT case
      VersionedDatasetHandle versionedDatasetHandle = handle.unwrap(VersionedDatasetHandle.class);
      VersionedDatasetId.Builder builder = new VersionedDatasetId.Builder()
        .setTableKey(handle.getDatasetPath().getComponents())
        .setContentId(versionedDatasetHandle.getContentId())
        .setTableVersionContext(this.options.getTimeTravelRequest() != null ?
          TableVersionContext.of(this.options.getTimeTravelRequest()) :
          TableVersionContext.of(this.options.versionedDatasetAccessOptions().getVersionContext()));
      VersionedDatasetId versionedDatasetId = builder.build();
      toReturn.setId(new EntityId(versionedDatasetId.asString()));
    } else if (this.options.getTimeTravelRequest() != null) {
      //AT TIMESTAMP/SNAPSHOT case
      VersionedDatasetId.Builder builder = new VersionedDatasetId.Builder()
        .setTableKey(handle.getDatasetPath().getComponents())
        .setContentId(null)
        .setTableVersionContext(TableVersionContext.of(this.options.getTimeTravelRequest()));
      VersionedDatasetId versionedDatasetId = builder.build();
      toReturn.setId(new EntityId(versionedDatasetId.asString()));
    }
    //Otherwise use the generated datasetId in other cases
    final DatasetMetadata datasetMetadata;
    try {
      datasetMetadata = plugin.getDatasetMetadata(handle, listingSupplier.get(),
          options.asGetMetadataOptions(currentConfig));
    } catch (ConnectorException e) {
      throw UserException.validationError(e)
          .buildSilently();
    }

    MetadataObjectsUtils.overrideExtended(toReturn, datasetMetadata, Optional.empty(),
        recordCountFromSplits, options.maxMetadataLeafColumns());
    return toReturn;
  }
}
