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
package com.dremio.exec.store.iceberg;

import java.util.List;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.types.Types;

import com.dremio.exec.record.BatchSchema;
import com.dremio.io.file.Path;
import com.google.common.base.Preconditions;

/**
 * Interface to Iceberg table operations
 */
public class IcebergOperation {
  private IcebergOperation(Type opType,
                          Path tableFolder,
                          BatchSchema batchSchema,
                          List<String> partitionColumnNames,
                          Configuration configuration) {
    this.opType = opType;
    this.tableFolder = tableFolder;
    this.batchSchema = batchSchema;
    this.configuration = configuration;
    this.icebergCatalog = new IcebergCatalog(this.tableFolder.toString(), this.configuration);
    this.partitionColumnNames = partitionColumnNames;
  }

  public enum Type {
    CREATE,
    INSERT,
    TRUNCATE,
    METADATA
  }

  private final Type opType;
  private IcebergCatalog icebergCatalog;
  private Path tableFolder;
  private BatchSchema batchSchema;
  private Configuration configuration;
  private List<String> partitionColumnNames;

  /**
   * This method starts create table operation
   */
  private IcebergOpCommitter beginCreateTable() {
    Preconditions.checkState(icebergCatalog != null, "Unexpected state");
    Preconditions.checkState(tableFolder != null, "Invalid path found");
    Preconditions.checkState(batchSchema != null, "Schema must be present");
    icebergCatalog.beginCreateTable(batchSchema, partitionColumnNames);
    return new IcebergTableCreationCommitter(this);
  }

  /**
   * This method starts insert table operation
   */
  private IcebergOpCommitter beginInsertTable() {
    Preconditions.checkState(icebergCatalog != null, "Unexpected state");
    Preconditions.checkState(tableFolder != null, "Invalid path found");
    Preconditions.checkState(batchSchema != null, "Schema must be present");
    icebergCatalog.beginInsertTable();
    return new IcebergInsertOperationCommitter(this);
  }

  /**
   * Calling commit will add data files and commits the transaction
   */
  public void commit() {
    Preconditions.checkState(icebergCatalog != null, "Unexpected state found");
    switch (opType) {
      case CREATE:
        icebergCatalog.endCreateTable();
        break;
      case INSERT:
        icebergCatalog.endInsertTable();
        break;
    }
  }

  public void consumeData(List<DataFile> dataFiles) {
    Preconditions.checkState(icebergCatalog != null, "Unexpected state found");
    icebergCatalog.consumeData(dataFiles);
  }

  public static IcebergOpCommitter getCreateTableCommitter(Path tableFolder,
                                                           BatchSchema batchSchema,
                                                           List<String> partitionColumnNames,
                                                           Configuration configuration) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.CREATE,
      tableFolder, batchSchema, partitionColumnNames, configuration);
    return icebergOperation.beginCreateTable();
  }

  public static IcebergOpCommitter getInsertTableCommitter(Path tableFolder,
                                                           BatchSchema batchSchema,
                                                           List<String> partitionColumnNames,
                                                           Configuration configuration) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.INSERT,
      tableFolder, batchSchema, partitionColumnNames, configuration);
    return icebergOperation.beginInsertTable();
  }

  private void truncateTable() {
    icebergCatalog.truncateTable();
  }

  public static void truncateTable(Path tableFolder, Configuration configuration) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.TRUNCATE,
      tableFolder, null, null, configuration);
    icebergOperation.truncateTable();
  }

  public static void addColumns(Path path, List<Types.NestedField> columnsToAdd, Configuration fsConf) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.METADATA,
        path, null, null, fsConf);
    icebergOperation.addColumns(columnsToAdd);
  }

  private void addColumns(List<Types.NestedField> columnsToAdd) {
    icebergCatalog.addColumns(columnsToAdd);
  }

  public static void dropColumn(Path path, String columnToDrop, Configuration fsConf) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.METADATA,
        path, null, null, fsConf);
    icebergOperation.dropColumn(columnToDrop);
  }

  private void dropColumn(String columnToDrop) {
    icebergCatalog.dropColumn(columnToDrop);
  }

  public static void changeColumn(Path path, String columnToChange, Field newDef, Configuration fsConf) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.METADATA,
        path, null, null, fsConf);
    icebergOperation.changeColumn(columnToChange, SchemaConverter.toIcebergColumn(newDef));
  }

  private void changeColumn(String columnToChange, Types.NestedField newDef) {
    icebergCatalog.changeColumn(columnToChange, newDef);
  }

  // TODO: currently this function is called from unit tests only.
  //  Need to revisit it when we implement alter table rename column command
  public static void renameColumn(Path path, String name, String newName, Configuration fsConf) {
    IcebergOperation icebergOperation = new IcebergOperation(Type.METADATA,
      path, null, null, fsConf);
    icebergOperation.renameColumn(name, newName);
  }

  private void renameColumn(String name, String newName) {
    icebergCatalog.renameColumn(name, newName);
  }

}
