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
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import com.dremio.exec.catalog.AlterTableOption;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.ColumnOperations;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.io.file.FileSystem;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;

/**
 * This interface is the entry point to Iceberg tables
 */
public interface IcebergModel {

  IcebergOpCommitter getCreateTableCommitter(String tableName, IcebergTableIdentifier tableIdentifier,
                                             BatchSchema batchSchema,
                                             List<String> partitionColumnNames, OperatorStats operatorStats, PartitionSpec partitionSpec);

  /**
   * Get Iceberg Op committer for Insert command
   *
   * @param tableIdentifier Table identifier
   * @param operatorStats
   * @return Insert committer
   */
  IcebergOpCommitter getInsertTableCommitter(IcebergTableIdentifier tableIdentifier, OperatorStats operatorStats);

  /**
   * Get committer for Full metadata refresh
   *
   * @param tableName
   * @param datasetPath
   * @param tableLocation
   * @param tableIdentifier
   * @param batchSchema
   * @param partitionColumnNames
   * @param operatorStats
   * @return
   */
  IcebergOpCommitter getFullMetadataRefreshCommitter(String tableName, List<String> datasetPath, String tableLocation,
                                                     String tableUuid, IcebergTableIdentifier tableIdentifier,
                                                     BatchSchema batchSchema, List<String> partitionColumnNames,
                                                     DatasetConfig datasetConfig, OperatorStats operatorStats, PartitionSpec partitionSpec);

  /**
   * Get Iceberg Op committer for Metadata Incremental Refresh command
   *
   * @param opContext
   * @param tableName
   * @param datasetPath
   * @param tableLocation
   * @param tableUuid
   * @param tableIdentifier
   * @param batchSchema
   * @param partitionColumnNames
   * @param forFileSystem
   * @param datasetConfig
   * @return
   */
  IcebergOpCommitter getIncrementalMetadataRefreshCommitter(OperatorContext opContext, String tableName, List<String> datasetPath, String tableLocation,
                                                            String tableUuid, IcebergTableIdentifier tableIdentifier,
                                                            BatchSchema batchSchema, List<String> partitionColumnNames,
                                                            boolean forFileSystem, DatasetConfig datasetConfig);

  /**
   * Get Iceberg Op committer for Alter command
   *
   * @param tableIdentifier Table identifier
   * @return Alter committer
   */
  IcebergOpCommitter getAlterTableCommitter(IcebergTableIdentifier tableIdentifier, ColumnOperations.AlterOperationType alterOperationType, BatchSchema droppedColumns, BatchSchema updatedColumns,
                                            String columnName, List<Field> columnTypes);

  /**
   * Iceberg Op committer for DML (Delete, Merge, Update) commands
   *
   * @param operatorStats
   * @param tableIdentifier
   * @param datasetConfig
   * @return
   */
  IcebergOpCommitter getDmlCommitter(OperatorStats operatorStats, IcebergTableIdentifier tableIdentifier, DatasetConfig datasetConfig);

  /**
   * Get Iceberg Op committer for primary key command
   *
   * @param tableIdentifier Table identifier
   * @param columns         Primary key column fields
   * @return Primary key update committer
   */
  IcebergOpCommitter getPrimaryKeyUpdateCommitter(IcebergTableIdentifier tableIdentifier, List<Field> columns);

  /**
   * Iceberg Op committer for OPTIMIZE TABLE command
   */
  IcebergOpCommitter getOptimizeCommitter(OperatorStats operatorStats, IcebergTableIdentifier tableIdentifier,
                                          DatasetConfig datasetConfig, Long minInputFilesBeforeOptimize,
                                          Long snapshotId, IcebergTableProps icebergTableProps, FileSystem fs);

  /**
   * Roll table back to the older snapshot.
   *
   * @param tableIdentifier table identifier
   * @param rollbackOption  rollback table option
   */
  void rollbackTable(IcebergTableIdentifier tableIdentifier, RollbackOption rollbackOption);

  /**
   * Expire table's snapshots, and return live snapshots after expiry and their manifest list file paths
   */
  Map<Long, String> expireSnapshots(IcebergTableIdentifier tableIdentifier, long olderThanInMillis, int retainLast);

  /**
   * Truncate a table
   *
   * @param tableIdentifier table identifier
   */
  void truncateTable(IcebergTableIdentifier tableIdentifier);


  void deleteTable(IcebergTableIdentifier tableIdentifier);

  void alterTable(IcebergTableIdentifier tableIdentifier, AlterTableOption alterTableOption);

  void deleteTableRootPointer(IcebergTableIdentifier tableIdentifier);


  /**
   * Add columns to a table
   *
   * @param tableIdentifier table identifier
   * @param columnsToAdd    list of columns to add
   * @return New root pointer for iceberg table
   */
  String addColumns(IcebergTableIdentifier tableIdentifier, List<Types.NestedField> columnsToAdd);

  /**
   * Drop a column from a table
   *
   * @param tableIdentifier table identifier
   * @param columnToDrop    Column name to drop
   * @return New root pointer for iceberg table
   */
  String dropColumn(IcebergTableIdentifier tableIdentifier, String columnToDrop);

  /**
   * Change column type of a table
   *
   * @param tableIdentifier table identifier
   * @param columnToChange  existing column name
   * @param newDef          new type
   * @return New root pointer for iceberg table
   */
  String changeColumn(IcebergTableIdentifier tableIdentifier, String columnToChange, Field newDef);

  /**
   * Rename an existing column of a table
   *
   * @param tableIdentifier table identifier
   * @param name            existing column name
   * @param newName         new column name
   * @return New root pointer for iceberg table
   */
  String renameColumn(IcebergTableIdentifier tableIdentifier, String name, String newName);

  /**
   * @param tableIdentifier table identifier
   * @param columns         primary key column fields
   * @return New root pointer for iceberg table
   */
  String updatePrimaryKey(IcebergTableIdentifier tableIdentifier, List<Field> columns);

  /**
   * Load and return an Iceberg table
   *
   * @param tableIdentifier table identifier
   * @return Iceberg table
   */
  Table getIcebergTable(IcebergTableIdentifier tableIdentifier);

  /**
   * Get table identifer
   *
   * @param rootFolder path to root folder of the table
   * @return table identifier
   */
  IcebergTableIdentifier getTableIdentifier(String rootFolder);

  /**
   * Returns an instance of Iceberg table loader
   *
   * @param tableIdentifier table identifier
   * @return An instance of Iceberg table loader
   */
  IcebergTableLoader getIcebergTableLoader(IcebergTableIdentifier tableIdentifier);

  /**
   * Finds the value of a table property
   */
  long propertyAsLong(IcebergTableIdentifier tableIdentifier, String propertyName, long defaultValue);

  /**
   * Fetch and reset to the latest version context
   */
  default void refreshVersionContext() {
    // do nothing
  }
}
