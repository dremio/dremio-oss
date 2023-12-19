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


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.proto.NameSpaceContainer;
import com.dremio.service.namespace.space.proto.FolderConfig;

/**
 * Given a table's logical name should interact with the KV-store to get schema, partition columns and other metadata
 */
public class UnlimitedSplitsMetadataProvider {
  private static final Logger logger = LoggerFactory.getLogger(UnlimitedSplitsMetadataProvider.class);
  private final NamespaceKey tableNSKey;
  private boolean metadataExists = false;

  private String tableUuid = UUID.randomUUID().toString();
  private BatchSchema schema = BatchSchema.EMPTY;
  private List<String> partitionCols = new ArrayList<>();
  private DatasetConfig config;

  private final NamespaceService nsService;

  public UnlimitedSplitsMetadataProvider(SqlHandlerConfig config, NamespaceKey tableNSKey) {
    this.tableNSKey = tableNSKey;
    nsService = config.getContext().getSystemNamespaceService();
    evaluateExistingMetadata();
  }

  private void createParentEntities() {
    NamespaceKey folderKey = tableNSKey;
    // use an ArrayList - reverse uses swap
    List<NamespaceKey> missingParentEntities = new ArrayList<>();
    while (folderKey.hasParent()) {
      folderKey = folderKey.getParent();
      if (!nsService.exists(folderKey)) {
        logger.info("Missing parent entity {} for {}", folderKey, tableNSKey);
        missingParentEntities.add(folderKey);
      }
    }

    Collections.reverse(missingParentEntities);
    for(NamespaceKey missingParent : missingParentEntities) {
      try {
        logger.info("Creating parent entity {} for {}", missingParent, tableNSKey);
        nsService.addOrUpdateFolder(missingParent,
          new FolderConfig()
            .setName(missingParent.getName())
            .setFullPathList(missingParent.getPathComponents())
        );
      } catch (NamespaceException namespaceException) {
        // suppress exception
        logger.warn("Unable to create parent entity {}", missingParent);
      }
    }
  }

  private void checkIfDatasetExists(boolean createParentFolders) throws NamespaceException {
    boolean existsCheckPassed = false;

    try {
      if (nsService.exists(tableNSKey, NameSpaceContainer.Type.DATASET)) {
        existsCheckPassed = true;
        config = nsService.getDataset(tableNSKey);
      }
    } catch (NamespaceException e) {
      // due to a bug in an earlier version, exists check returned true while getDataset() threw an exception
      // because parent entities were deleted
      if (existsCheckPassed && createParentFolders) {
        logger.info("Trying to create parent folders for {}", tableNSKey);
        createParentEntities();
        checkIfDatasetExists(false);
      } else {
        // rethrow the exception
        throw e;
      }
    }
  }

  private void evaluateExistingMetadata() {
    config = null;

    try {
      checkIfDatasetExists(true);
    } catch (NamespaceException e) {
      String errorMessage = String.format("Error while getting metadata for [%s].", tableNSKey.getSchemaPath());
      logger.error(errorMessage, e);
      throw new RuntimeException(errorMessage, e);
    }

    if (config == null) {
      logger.info("Table metadata for {} not found", tableNSKey.getSchemaPath());
      return;
    } else if (config.getPhysicalDataset() == null
      || !Boolean.TRUE.equals(config.getPhysicalDataset().getIcebergMetadataEnabled())) {
      logger.info("Forgetting current metadata for {}, it'll be re-promoted to use Iceberg to store metadata.", tableNSKey);
      return;
    }

    schema = BatchSchema.deserialize(config.getRecordSchema());
    String tableLocation = config.getPhysicalDataset().getIcebergMetadata().getMetadataFileLocation();
    tableUuid = config.getPhysicalDataset().getIcebergMetadata().getTableUuid();
    partitionCols = Optional.ofNullable(config.getReadDefinition().getPartitionColumnsList()).orElse(Collections.EMPTY_LIST);
    metadataExists = true;
    logger.info("Table metadata found for {}, at {}", tableNSKey.getSchemaPath(), tableLocation);
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

  public List<String> getPartitionColumns() {
    return partitionCols;
  }

  public DatasetConfig getDatasetConfig() {
    return config;
  }

  public void resetMetadata() {
    // get dataset config again from namespace service and reinitialize schema and partitionCols fields
    evaluateExistingMetadata();
  }

}
