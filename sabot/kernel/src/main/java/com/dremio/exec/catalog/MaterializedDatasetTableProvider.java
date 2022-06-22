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
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf;
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

  public MaterializedDatasetTableProvider(
      DatasetConfig currentConfig,
      DatasetHandle handle,
      StoragePlugin plugin,
      StoragePluginId pluginId,
      SchemaConfig schemaConfig,
      DatasetRetrievalOptions options
  ) {
    this.currentConfig = currentConfig;
    this.handle = handle;
    this.plugin = plugin;
    this.pluginId = pluginId;
    this.schemaConfig = schemaConfig;
    this.options = options;
  }

  @Override
  public MaterializedDatasetTable get() {
    final Supplier<PartitionChunkListing> partitionChunkListing = Suppliers.memoize(this::getPartitionChunkListing);

    final Supplier<List<PartitionProtobuf.PartitionChunk>> partitionChunks =
        Suppliers.memoize(() -> toPartitionChunks(partitionChunkListing));

    final Supplier<DatasetConfig> datasetConfig = Suppliers.memoize(
        () -> createDatasetConfig(partitionChunkListing, partitionChunks));

    final boolean timeTravel = options.getTimeTravelRequest() != null;
    return new MaterializedDatasetTable(MetadataObjectsUtils.toNamespaceKey(handle.getDatasetPath()), pluginId,
        schemaConfig.getUserName(), datasetConfig, partitionChunks,
        schemaConfig.getOptions().getOption(PlannerSettings.FULL_NESTED_SCHEMA_SUPPORT), timeTravel);
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
