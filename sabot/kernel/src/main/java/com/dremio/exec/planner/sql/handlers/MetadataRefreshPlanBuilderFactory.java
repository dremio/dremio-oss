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
package com.dremio.exec.planner.sql.handlers;

import static com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder.retrievalOptionsForPartitions;

import java.io.IOException;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.exceptions.UserException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.SourceMetadata;
import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.catalog.MetadataObjectsUtils;
import com.dremio.exec.planner.sql.handlers.refresh.AbstractRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.MetadataRefreshPlanBuilder;
import com.dremio.exec.planner.sql.handlers.refresh.UnlimitedSplitsMetadataProvider;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.store.DatasetRetrievalOptions;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.SupportsInternalIcebergTable;
import com.dremio.service.namespace.NamespaceKey;

import io.opentelemetry.instrumentation.annotations.WithSpan;

public class MetadataRefreshPlanBuilderFactory {

  private static final Logger logger = LoggerFactory.getLogger(MetadataRefreshPlanBuilderFactory.class);

  @WithSpan
  public static MetadataRefreshPlanBuilder getPlanBuilder(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset)
    throws IOException {

    SupportsInternalIcebergTable plugin = getPlugin(config, sqlRefreshDataset);
    final boolean isHiveDataset = getPlugin(config, sqlRefreshDataset).canGetDatasetMetadataInCoordinator();

    Catalog catalog = config.getContext().getCatalog();
    NamespaceKey nsKey = catalog.resolveSingle(new NamespaceKey(sqlRefreshDataset.getTable().names));

    if (isHiveDataset) {
      Optional<DatasetHandle> datasetHandle = ((SourceMetadata) plugin).getDatasetHandle(MetadataObjectsUtils.toEntityPath(nsKey));
      if (datasetHandle.isPresent()) {
        nsKey = MetadataObjectsUtils.toNamespaceKey(datasetHandle.get().getDatasetPath());
      }
    }

    UnlimitedSplitsMetadataProvider metadataProvider = new UnlimitedSplitsMetadataProvider(config, nsKey);
    boolean isFullRefresh = !metadataProvider.doesMetadataExist();

    if (isFullRefresh && sqlRefreshDataset.isPartialRefresh()) {
        throw new UnsupportedOperationException("Selective refresh is not allowed on unknown datasets. Please run full refresh on " + nsKey.getSchemaPath());
    }
    AbstractRefreshPlanBuilder planBuilder = plugin.createRefreshDatasetPlanBuilder(config, sqlRefreshDataset, metadataProvider, isFullRefresh);

    DatasetRetrievalOptions retrievalOptions = retrievalOptionsForPartitions(config, sqlRefreshDataset.getPartition());
    PartitionChunkListing chunks = planBuilder.listPartitionChunks(retrievalOptions);
    planBuilder.setupMetadataForPlanning(chunks, retrievalOptions);
    if (planBuilder.updateDatasetConfigWithIcebergMetadataIfNecessary()) {
      // here if datasetConfig is not in sync with iceberg metadata and new datasetConfig is saved in catalog
      metadataProvider.resetMetadata();
      PartitionChunkListing newChunksListing = planBuilder.listPartitionChunks(retrievalOptions);
      planBuilder.setupMetadataForPlanning(newChunksListing, retrievalOptions); // do this again since datasetConfig got modified
    }
    return planBuilder;
  }

  private static SupportsInternalIcebergTable getPlugin(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset) {
    Catalog catalog = config.getContext().getCatalog();
    NamespaceKey key = catalog.resolveSingle(new NamespaceKey(sqlRefreshDataset.getTable().names));

    StoragePlugin plugin = catalog.getSource(key.getRoot());
    if (plugin instanceof SupportsInternalIcebergTable) {
      return (SupportsInternalIcebergTable) plugin;
    } else {
      throw UserException.validationError()
        .message("Source identified was invalid type. REFRESH DATASET is only supported on sources that can contain files or folders")
        .build(logger);
    }
  }

}
