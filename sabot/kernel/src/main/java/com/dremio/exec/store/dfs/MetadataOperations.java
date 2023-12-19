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
package com.dremio.exec.store.dfs;

import static com.dremio.exec.store.metadatarefresh.MetadataRefreshExecConstants.METADATA_STORAGE_PLUGIN_NAME;

import java.util.Map;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.users.SystemUser;

import io.opentelemetry.instrumentation.annotations.WithSpan;
import io.protostuff.ByteString;

/**
 * Base class to perform iceberg metadata operations
 */
abstract class MetadataOperations {
  protected final DatasetConfig datasetConfig;
  protected final SabotContext context;
  protected final StoragePlugin storagePlugin;
  protected final NamespaceKey table;
  protected final SchemaConfig schemaConfig;
  protected final IcebergModel model;
  protected final Path path;

  public MetadataOperations(DatasetConfig datasetConfig,
                            SabotContext context,
                            NamespaceKey table,
                            SchemaConfig schemaConfig,
                            IcebergModel model,
                            Path path,
                            StoragePlugin storagePlugin) {
    this.datasetConfig = datasetConfig;
    this.context = context;
    this.table = table;
    this.schemaConfig = schemaConfig;
    this.model = model;
    this.path = path;
    this.storagePlugin = storagePlugin;
  }

  protected void checkUserSchemaEnabled() {
    if (!context.getOptionManager().getOption(ExecConstants.ENABLE_INTERNAL_SCHEMA)) {
      throw UserException.parseError()
        .message("Modifications to internal schema's are not allowed. " +
          "Contact dremio support to enable option user managed schema's ")
        .buildSilently();
    }
  }

  protected String getMetadataTableName() {
    return datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
  }

  @WithSpan
  protected void checkAndRepair() {
    RepairKvstoreFromIcebergMetadata repairOperation = new RepairKvstoreFromIcebergMetadata(
      datasetConfig,
      context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME),
      context.getNamespaceService(SystemUser.SYSTEM_USERNAME),
      storagePlugin,
      context.getOptionManager());
    repairOperation.checkAndRepairDatasetWithQueryRetry();
  }

  protected void updateDatasetConfigWithIcebergMetadata(String newRootPointer,
                                                        long snapshotId,
                                                        Map<Integer, PartitionSpec> partitionSpecMap,
                                                        Schema schema) {
    // Snapshot should not change with change in schema but still update it
    IcebergMetadata icebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    icebergMetadata.setMetadataFileLocation(newRootPointer);
    icebergMetadata.setSnapshotId(snapshotId);
    byte[] specs = IcebergSerDe.serializePartitionSpecAsJsonMap(partitionSpecMap);
    icebergMetadata.setPartitionSpecsJsonMap(ByteString.copyFrom(specs));
    icebergMetadata.setJsonSchema(IcebergSerDe.serializedSchemaAsJson(schema));
    datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
  }

  protected static void save(NamespaceKey table,
                             DatasetConfig datasetConfig,
                             String userName,
                             SabotContext context) {
    try {
      context.getNamespaceService(userName).addOrUpdateDataset(table, datasetConfig);
    } catch (NamespaceException e) {
      throw UserException.validationError(e)
        .message("Failure while updating dataset")
        .buildSilently();
    }
  }
}
