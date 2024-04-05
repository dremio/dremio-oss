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

import com.dremio.common.exceptions.UserException;
import com.dremio.common.types.SupportsTypeCoercionsAndUpPromotions;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.util.BatchSchemaDiff;
import com.dremio.exec.util.BatchSchemaDiffer;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Table;

public class ColumnOperations extends MetadataOperations {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ColumnOperations.class);

  public static String DREMIO_UPDATE_COLUMNS = "dremio.updatedColumns";
  public static String DREMIO_DROPPED_COLUMNS = "dremio.droppedColumns";

  public enum AlterOperationType {
    CHANGE,
    ADD,
    DROP
  }

  protected BatchSchema newDroppedCols = BatchSchema.EMPTY;
  protected BatchSchema newModifiedCols = BatchSchema.EMPTY;
  protected BatchSchema newSchema = BatchSchema.EMPTY;

  public ColumnOperations(
      NamespaceKey table,
      SabotContext context,
      DatasetConfig datasetConfig,
      SchemaConfig schemaConfig,
      IcebergModel model,
      Path path,
      StoragePlugin storagePlugin) {
    super(datasetConfig, context, table, schemaConfig, model, path, storagePlugin);
  }

  protected void checkPartitionColumnsValidation(List<String> fieldsToAlter) {
    fieldsToAlter = fieldsToAlter.stream().map(String::toLowerCase).collect(Collectors.toList());
    List<String> finalFieldsToAlter = fieldsToAlter;

    if (datasetConfig.getReadDefinition().getPartitionColumnsList() == null) {
      return;
    }
    datasetConfig
        .getReadDefinition()
        .getPartitionColumnsList()
        .forEach(
            x -> {
              if (finalFieldsToAlter.contains(x.toLowerCase())) {
                throw UserException.parseError()
                    .message(
                        String.format(
                            "Modifications to partition columns are not allowed. Column %s is a partition column",
                            x))
                    .buildSilently();
              }
            });
  }

  protected UserDefinedSchemaSettings getUserDefinedSettings() {
    UserDefinedSchemaSettings internalSchemaSettings =
        datasetConfig.getPhysicalDataset().getInternalSchemaSettings();

    if (internalSchemaSettings == null) {
      internalSchemaSettings = new UserDefinedSchemaSettings();
    }

    return internalSchemaSettings;
  }

  protected void saveInKvStore() {
    datasetConfig.setRecordSchema(newSchema.toByteString());
    datasetConfig
        .getPhysicalDataset()
        .setInternalSchemaSettings(
            getUserDefinedSettings()
                .setDroppedColumns(newDroppedCols.toByteString())
                .setModifiedColumns(newModifiedCols.toByteString()));
    save(table, datasetConfig, schemaConfig.getUserName(), context);
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
    batchSchemaDiffer.setAllowComplexToPrimitiveConversions(
        storagePlugin instanceof SupportsTypeCoercionsAndUpPromotions
            && ((SupportsTypeCoercionsAndUpPromotions) storagePlugin)
                .isComplexToVarcharCoercionSupported());
    BatchSchemaDiff diff = batchSchemaDiffer.diff(oldSchema.getFields(), newSchema.getFields());

    newDroppedCols = new BatchSchema(diff.getDroppedFields());
    newModifiedCols = new BatchSchema(diff.getModifiedFields());
    BatchSchema newAddedCols = new BatchSchema(diff.getAddedFields());

    // oldDroppedColumns = oldDroppedColumns - newAddedCols;
    // oldModifiedColumns = oldModifiedColumns - newDroppedCols;
    // newModifiedCols = newModifiedCols + newAddedCols;

    // If a dropped column is being modified now then remove it from the dropped column list
    oldDroppedCols = oldDroppedCols.difference(newAddedCols);

    // If a modified column is being dropped now then remove it from the modified list
    oldModifiedCols = oldModifiedCols.difference(newDroppedCols);

    newDroppedCols = newDroppedCols.mergeWithRetainOld(oldDroppedCols);
    newModifiedCols = newModifiedCols.mergeWithRetainOld(oldModifiedCols);
    newModifiedCols = newModifiedCols.mergeWithRetainOld(newAddedCols);
  }

  protected void reloadSchemaAndDroppedAndUpdatedColumns(Table icebergTable) {
    newSchema =
        SchemaConverter.getBuilder()
            .setMapTypeEnabled(
                context.getOptionManager().getOption(ExecConstants.ENABLE_MAP_DATA_TYPE))
            .build()
            .fromIceberg(icebergTable.schema());
    com.fasterxml.jackson.databind.ObjectMapper mapper =
        new com.fasterxml.jackson.databind.ObjectMapper();
    try {
      newModifiedCols =
          mapper.readValue(
              icebergTable
                  .properties()
                  .getOrDefault(ColumnOperations.DREMIO_UPDATE_COLUMNS, BatchSchema.EMPTY.toJson()),
              BatchSchema.class);
      newDroppedCols =
          mapper.readValue(
              icebergTable
                  .properties()
                  .getOrDefault(
                      ColumnOperations.DREMIO_DROPPED_COLUMNS, BatchSchema.EMPTY.toJson()),
              BatchSchema.class);
    } catch (JsonProcessingException e) {
      String message =
          "Unable to retrieve dropped and modified columns from iceberg" + e.getMessage();
      logger.error(message);
      throw UserException.dataReadError().addContext(message).build(logger);
    }
  }
}
