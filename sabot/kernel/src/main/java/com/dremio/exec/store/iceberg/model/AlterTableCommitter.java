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
package com.dremio.exec.store.iceberg.model;

import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.ColumnOperations;

public class AlterTableCommitter implements IcebergOpCommitter  {

  private IcebergCommand command;
  private ColumnOperations.AlterOperationType operationType;
  private String rootPointer;
  private String columnName;
  private List<Field> columnTypes;
  private BatchSchema droppedColumns;
  private BatchSchema updatedColumns;
  private Map<Integer, PartitionSpec> specMap;
  private Table table;

  public AlterTableCommitter(IcebergCommand icebergCommand, ColumnOperations.AlterOperationType alterOperationType, String columnName, List<Field> columnTypes, BatchSchema droppedColumns, BatchSchema updatedColumns) {
    this.command = icebergCommand;
    this.operationType = alterOperationType;
    this.columnName = columnName;
    this.columnTypes = columnTypes;
    this.droppedColumns = droppedColumns;
    this.updatedColumns = updatedColumns;
    command.beginAlterTableTransaction();
  }

  @Override
  public Snapshot commit() {
    switch (operationType) {
      case DROP:
        command.dropColumnInternalTable(columnName);
        break;
      case ADD:
        command.addColumnsInternalTable(columnTypes);
        break;
      case CHANGE:
        command.changeColumnForInternalTable(columnName, columnTypes.get(0));
        break;
    }
    command.updatePropertiesMap(droppedColumns, updatedColumns);
    table = command.endAlterTableTransaction();
    rootPointer = command.getRootPointer();
    specMap = command.getPartitionSpecMap();
    return table.currentSnapshot();
  }

  @Override
  public void consumeDeleteDataFile(DataFile icebergDeleteDatafile) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void consumeManifestFile(ManifestFile manifestFile) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateSchema(BatchSchema newSchema) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getRootPointer() {
    return rootPointer;
  }

  @Override
  public Map<Integer, PartitionSpec> getCurrentSpecMap() {
    return specMap;
  }

  @Override
  public boolean isIcebergTableUpdated() {
    return false;
  }

  public Table getIcebergTable() {
    return table;
  }
}
