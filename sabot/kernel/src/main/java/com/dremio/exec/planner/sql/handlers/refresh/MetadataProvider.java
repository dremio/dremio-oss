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
package com.dremio.exec.planner.sql.handlers.refresh;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.arrow.util.Preconditions;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.catalog.Catalog;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.parser.SqlRefreshDataset;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.service.catalog.DatasetCatalogServiceGrpc;
import com.dremio.service.catalog.GetDatasetRequest;
import com.dremio.service.catalog.UpdatableDatasetConfigFields;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

/**
 * Given a table's logical name should interact with the KV-store to get schema
 * Abstractions can be done extending this unit for different branches incremental and partial refresh use-cases.
 */
public class MetadataProvider {
  private static final Logger logger = LoggerFactory.getLogger(MetadataProvider.class);

  private final NamespaceKey tableNSKey;
  private boolean metadataExists = false;

  private String tableUuid = UUID.randomUUID().toString();
  private BatchSchema schema = BatchSchema.EMPTY;
  private List<String> partitionCols = new ArrayList<>();

  public MetadataProvider(SqlHandlerConfig config, SqlRefreshDataset sqlRefreshDataset) {
    final Catalog catalog = config.getContext().getCatalog();
    tableNSKey = catalog.resolveSingle(new NamespaceKey(sqlRefreshDataset.getTable().names));
    evaluateExistingMetadata(config.getContext().getCatalogService());
  }

  private void evaluateExistingMetadata(CatalogService catalog) {
    try {
      // Catalog entry isn't available. Hence, acquiring KV details from gRPC call. TODO: use catalog after DX-33403 is fixed.
      //final DremioTable table = catalog.getTable(tableNSKey); Always returning null

      FileSystemPlugin plugin = (FileSystemPlugin<?>) catalog.getSource("__metadata");
      DatasetCatalogServiceGrpc.DatasetCatalogServiceBlockingStub datasetCatalogClient = plugin.getContext().getDatasetCatalogBlockingStub().get();
      GetDatasetRequest req = GetDatasetRequest.newBuilder().addDatasetPath(tableNSKey.getSchemaPath()).build();
      UpdatableDatasetConfigFields datasetFields = datasetCatalogClient.getDataset(req);

      logger.info("Table metadata for {} found, setting entities", tableNSKey.getSchemaPath());
      metadataExists = true;
      schema = BatchSchema.deserialize(datasetFields.getBatchSchema().toByteArray());
      partitionCols = ImmutableList.copyOf(datasetFields.getReadDefinition().getPartitionColumnsList().listIterator());
      String tableLocation = datasetFields.getFileFormat().getLocation();
      Preconditions.checkState(StringUtils.isNotBlank(tableLocation), "Table location isn't identified");
      Path path = Paths.get(tableLocation);
      tableUuid = path.getFileName().toString();
    } catch (Exception e) {
      if (e.getMessage().contains("NOT_FOUND")) {
        logger.info("Table metadata for {} not found", tableNSKey.getSchemaPath());
      } else {
        logger.error("Error while evaluating existing metadata", e);
        throw new RuntimeException(e);
      }
    }
  }

  public Boolean doesMetadataExist() {
    return metadataExists;
  }

  public String getTableUUId() {
    return tableUuid;
  }

  public BatchSchema getTableSchema() {
    return schema;
  }

  public List<String> getPartitionColumn() {
    return partitionCols;
  }
}
