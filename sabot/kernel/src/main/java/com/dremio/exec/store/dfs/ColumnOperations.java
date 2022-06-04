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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.util.BatchSchemaDiff;
import com.dremio.exec.util.BatchSchemaDiffer;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.IcebergMetadata;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.users.SystemUser;
import com.fasterxml.jackson.core.JsonProcessingException;

import io.protostuff.ByteString;

public class ColumnOperations {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ColumnOperations.class);

  public static String DREMIO_UPDATE_COLUMNS = "dremio.updatedColumns";
  public static String DREMIO_DROPPED_COLUMNS = "dremio.droppedColumns";

  public enum AlterOperationType {
    CHANGE,
    ADD,
    DROP
  }


  protected final NamespaceKey table;
  protected final SabotContext context;
  protected final SchemaConfig schemaConfig;
  protected final IcebergModel model;
  protected final Path path;
  protected final DatasetConfig datasetConfig;
  protected StoragePlugin storagePlugin;

  protected BatchSchema newDroppedCols = BatchSchema.EMPTY;
  protected BatchSchema newModifiedCols = BatchSchema.EMPTY;
  protected BatchSchema newSchema = BatchSchema.EMPTY;

  public ColumnOperations(NamespaceKey table, SabotContext context, SchemaConfig schemaConfig, IcebergModel model, Path path, StoragePlugin storagePlugin) {
    this.table = table;
    this.context = context;
    this.schemaConfig = schemaConfig;
    this.model = model;
    this.path = path;
    try {
      this.datasetConfig = context.getNamespaceService(schemaConfig.getUserName()).getDataset(table);
    } catch (
      NamespaceException e) {
      throw UserException.dataReadError().message(String.format("Dataset %s not found", table.toString())).buildSilently();
    }
    this.storagePlugin = storagePlugin;
  }

  protected void checkUserSchemaEnabled() {
    if(!context.getOptionManager().getOption(ExecConstants.ENABLE_INTERNAL_SCHEMA)) {
      throw UserException.parseError()
        .message("Modifications to internal schema's are not allowed. Contact dremio support to enable option user managed schema's ")
        .buildSilently();
    }
  }

  protected void checkPartitionColumnsValidation(List<String> fieldsToAlter) {
    fieldsToAlter = fieldsToAlter.stream().map(String::toLowerCase).collect(Collectors.toList());
    List<String> finalFieldsToAlter = fieldsToAlter;
    datasetConfig.getReadDefinition().getPartitionColumnsList().stream().forEach(x -> {
      if(finalFieldsToAlter.contains(x.toLowerCase())) {
        throw UserException.parseError()
          .message(String.format("Modifications to partition columns are not allowed. Column %s is a partition column", x))
          .buildSilently();
      }
    });
  }

  protected String getMetadataTableName(SabotContext context, DatasetConfig datasetConfig) {
    String metadataTableName = datasetConfig.getPhysicalDataset().getIcebergMetadata().getTableUuid();
    return metadataTableName;
  }

  protected UserDefinedSchemaSettings getUserDefinedSettings() {
    UserDefinedSchemaSettings internalSchemaSettings = datasetConfig.getPhysicalDataset().getInternalSchemaSettings();

    if(internalSchemaSettings == null) {
      internalSchemaSettings = new UserDefinedSchemaSettings();
    }

    return internalSchemaSettings;
  }

  protected void saveInKvStore() {
    datasetConfig.setRecordSchema(newSchema.toByteString());
    datasetConfig.getPhysicalDataset().setInternalSchemaSettings(getUserDefinedSettings().setDroppedColumns(newDroppedCols.toByteString()).setModifiedColumns(newModifiedCols.toByteString()));
    try {
      context.getNamespaceService(schemaConfig.getUserName()).addOrUpdateDataset(table, datasetConfig);
    } catch(NamespaceException e) {
      throw new RuntimeException(e);
    }
  }

  protected void updateDatasetConfigWithIcebergMetadata(String newRootPointer, long snapshotId, Map<Integer, PartitionSpec> partitionSpecMap) { ;
    //Snapshot should not change with change in schema but still update it
    IcebergMetadata icebergMetadata = datasetConfig.getPhysicalDataset().getIcebergMetadata();
    icebergMetadata.setMetadataFileLocation(newRootPointer);
    icebergMetadata.setSnapshotId(snapshotId);
    byte[] specs = IcebergSerDe.serializePartitionSpecMap(partitionSpecMap);
    icebergMetadata.setPartitionSpecs(ByteString.copyFrom(specs));
    datasetConfig.getPhysicalDataset().setIcebergMetadata(icebergMetadata);
  }

  protected void computeDroppedAndUpdatedColumns(BatchSchema newSchema) {
    BatchSchema oldSchema = BatchSchema.deserialize(datasetConfig.getRecordSchema());

    UserDefinedSchemaSettings internalSchemaSettings = getUserDefinedSettings();

    BatchSchema oldDroppedCols = BatchSchema.EMPTY;
    if (internalSchemaSettings.getDroppedColumns() != null) {
      oldDroppedCols = BatchSchema.deserialize(internalSchemaSettings.getDroppedColumns());
    }

    BatchSchema oldModifiedCols = BatchSchema.EMPTY;
    if (internalSchemaSettings.getModifiedColumns() != null) {
      oldModifiedCols = BatchSchema.deserialize(internalSchemaSettings.getModifiedColumns());
    }

    BatchSchemaDiffer batchSchemaDiffer = new BatchSchemaDiffer();
    BatchSchemaDiff diff = batchSchemaDiffer.diff(oldSchema.getFields(), newSchema.getFields());

    newDroppedCols = new BatchSchema(diff.getDroppedFields());
    newModifiedCols = new BatchSchema(diff.getModifiedFields());
    BatchSchema newAddedCols = new BatchSchema(diff.getAddedFields());

    //oldDroppedColumns = oldDroppedColumns - newAddedCols;
    //oldModifiedColumns = oldModifiedColumns - newDroppedCols;
    //newModifiedCols = newModifiedCols + newAddedCols;

    //If a dropped column is being modified now then remove it from the dropped column list
    oldDroppedCols = oldDroppedCols.difference(newAddedCols);

    //If a modified column is being dropped now then remove it from the modified list
    oldModifiedCols = oldModifiedCols.difference(newDroppedCols);

    newDroppedCols = newDroppedCols.mergeWithRetainOld(oldDroppedCols);
    newModifiedCols = newModifiedCols.mergeWithRetainOld(oldModifiedCols);
    newModifiedCols = newModifiedCols.mergeWithRetainOld(newAddedCols);
  }

  protected void reloadSchemaAndDroppedAndUpdatedColumns(Table icebergTable) {
    newSchema = new SchemaConverter().fromIceberg(icebergTable.schema());
    com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    try {
      newModifiedCols = mapper.readValue(icebergTable.properties().getOrDefault(ColumnOperations.DREMIO_UPDATE_COLUMNS, BatchSchema.EMPTY.toJson()), BatchSchema.class);
      newDroppedCols = mapper.readValue(icebergTable.properties().getOrDefault(ColumnOperations.DREMIO_DROPPED_COLUMNS, BatchSchema.EMPTY.toJson()), BatchSchema.class);
    } catch (JsonProcessingException e) {
      String message = "Unable to retrieve dropped and modified columns from iceberg" + e.getMessage();
      logger.error(message);
      throw UserException.dataReadError().addContext(message).build(logger);
    }
  }

  protected void checkAndRepair() {
    RepairKvstoreFromIcebergMetadata repairOperation = new RepairKvstoreFromIcebergMetadata(datasetConfig, context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME), context.getNamespaceService(SystemUser.SYSTEM_USERNAME), storagePlugin);
    repairOperation.checkAndRepairDatasetWithQueryRetry();
  }
}
