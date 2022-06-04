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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.SchemaConfig;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.iceberg.model.AlterTableCommitter;
import com.dremio.exec.store.iceberg.model.IcebergModel;
import com.dremio.exec.store.iceberg.model.IcebergOpCommitter;
import com.dremio.exec.store.iceberg.model.IcebergTableIdentifier;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.DatasetHelper;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;

public class ChangeColumn extends ColumnOperations {

  public ChangeColumn(NamespaceKey table, SabotContext context, SchemaConfig schemaConfig, IcebergModel model, Path path,  StoragePlugin storagePlugin) {
    super(table, context, schemaConfig, model, path, storagePlugin);
  }

  public void performOperation(String columnToChange, Field fieldFromSql) {
    checkAndRepair();
    boolean isIcebergTable = DatasetHelper.isIcebergDataset(datasetConfig);

    if (isIcebergTable) {
      model.changeColumn(model.getTableIdentifier(path.toString()), columnToChange, fieldFromSql);
      return;
    }

    checkUserSchemaEnabled();
    checkPartitionColumnsValidation(ImmutableList.of(columnToChange));
    Boolean internalIcebergTable  = DatasetHelper.isInternalIcebergTable(datasetConfig);

    BatchSchema oldSchema = BatchSchema.deserialize(datasetConfig.getRecordSchema());
    newSchema = oldSchema.changeTypeTopLevel(fieldFromSql);
    computeDroppedAndUpdatedColumns(newSchema);

    if (internalIcebergTable) {
      String metadataTableName = getMetadataTableName(context, datasetConfig);
      FileSystemPlugin<?> metaStoragePlugin = context.getCatalogService().getSource(METADATA_STORAGE_PLUGIN_NAME);

      IcebergModel icebergModel = metaStoragePlugin.getIcebergModel();

      IcebergTableIdentifier tableIdentifier = icebergModel.getTableIdentifier(
        metaStoragePlugin.resolveTablePathToValidPath(metadataTableName).toString());

      IcebergOpCommitter opCommitter = icebergModel.getAlterTableCommitter(tableIdentifier, AlterOperationType.CHANGE, newDroppedCols, newModifiedCols, columnToChange, ImmutableList.of(fieldFromSql));
      Snapshot snapshot = opCommitter.commit();
      updateDatasetConfigWithIcebergMetadata(opCommitter.getRootPointer(), snapshot.snapshotId(), opCommitter.getCurrentSpecMap());

      Table icebergTable = ((AlterTableCommitter) opCommitter).getIcebergTable();
      reloadSchemaAndDroppedAndUpdatedColumns(icebergTable);
    }

    saveInKvStore();
  }
}
