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

import com.dremio.exec.catalog.PartitionSpecAlterOption;
import com.dremio.exec.catalog.RollbackOption;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.SnapshotEntry;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;

/** represents an Iceberg catalog */
public interface IcebergCommand {

  /**
   * Start of Create table command
   *
   * @param tableName name of the table
   * @param writerSchema schema of the table
   * @param partitionColumns partition specification of the table
   * @param tableParameters icebeg table parameters
   * @param sortOrder
   */
  void beginCreateTableTransaction(
      String tableName,
      BatchSchema writerSchema,
      List<String> partitionColumns,
      Map<String, String> tableParameters,
      PartitionSpec partitionSpec,
      SortOrder sortOrder);

  /** Registers an existing table to the catalog */
  void registerTable(TableMetadata tableMetadata);

  /** Start of a tansaction */
  void beginTransaction();

  /** End of a tansaction */
  Table endTransaction();

  /** Start the overwrite operation */
  void beginOverwrite(long snapshotId);

  /** start the RowDelta operation */
  void beginRowDelta(Long startingSnapshotId);

  /** Start overwrite operation with Serializable-level Isolation */
  void beginSerializableIsolationOverwrite(long snapshotId, Expression conflictDetectionFilter);

  /** Start row delta operation with Serializable-level Isolation */
  void beginSerializableIsolationRowDelta(
      CharSequenceSet referencedDataFiles,
      Long startingSnapshotId,
      Expression conflictDetectionFilter);

  /** Commit the overwrite operation */
  void finishOverwrite();

  /** commit the RowDelta operation */
  void finishRowDelta();

  /**
   * Performs rewrite operation and commits the transaction
   *
   * @param removedDataFiles
   * @param removedDeleteFiles
   * @param addedDataFiles
   * @param addedDeleteFiles
   * @return updated snapshot
   */
  Snapshot rewriteFiles(
      Set<DataFile> removedDataFiles,
      Set<DeleteFile> removedDeleteFiles,
      Set<DataFile> addedDataFiles,
      Set<DeleteFile> addedDeleteFiles,
      Long snapshotId);

  /**
   * Consumes list of deleted data files using Overwrite
   *
   * @param filePathsList list of DataFile entries
   */
  void consumeDeleteDataFilesWithOverwriteByPaths(List<String> filePathsList);

  /**
   * Consumes list of Manifest files using Overwrite
   *
   * @param filesList list of DataFile entries
   */
  void consumeManifestFilesWithOverwrite(List<ManifestFile> filesList);

  /** Start the delete operation */
  void beginDelete();

  /** Commit the delete operation */
  Snapshot finishDelete();

  /** Start the insert operation */
  void beginInsert();

  /** Finish the insert operation */
  Snapshot finishInsert();

  /**
   * Expire older snapshots, but don't clean orphan files.
   *
   * @return Live snapshots and their manifest list file paths
   */
  List<SnapshotEntry> expireSnapshots(
      long olderThanInMillis, int retainLast, boolean throwIcebergException);

  /**
   * Roll a table's data back to a specific snapshot identified either by id or before a given
   * timestamp.
   *
   * @param rollbackOption rollback table option
   */
  void rollback(RollbackOption rollbackOption);

  /**
   * consumes list of Manifest files as part of the current transaction
   *
   * @param filesList list of DataFile entries
   */
  void consumeManifestFiles(List<ManifestFile> filesList);

  /**
   * consumes list of data files to be deleted as a part of the current transaction
   *
   * @param filesList list of DataFile entries
   */
  void consumeDeleteDataFiles(List<DataFile> filesList);

  /**
   * consumes list of positional-delete files to be added as a part of the current transaction
   *
   * @param deleteFileList list of positional-DeleteFile entries
   */
  void consumePositionalDeleteFiles(List<DeleteFile> deleteFileList);

  /**
   * consumes list of data files to be added as a part of the current transaction.
   *
   * <p>Used during rowDelta operation (for Merge-On-Read)
   *
   * @param dataFileList
   */
  void consumeMergeOnReadDataFiles(List<DataFile> dataFileList);

  /**
   * consumes list of deleted data files by file paths as a part of the current transaction
   *
   * @param filePathsList list of data file paths
   */
  void consumeDeleteDataFilesByPaths(List<String> filePathsList);

  /**
   * consumes list of columns to be dropped as part of metadata refresh transaction. Used only in
   * new metadata refresh flow
   */
  void consumeDroppedColumns(List<Types.NestedField> columns);

  /**
   * consumes list of columns to be updated as part of metadata refresh transaction. Used only in
   * new metadata refresh flow
   */
  void consumeUpdatedColumns(List<Types.NestedField> columns);

  /**
   * consumes list of columns to be added to the schema as part of metadata refresh transaction.
   * Used only in new metadata refresh flow
   */
  void consumeAddedColumns(List<Types.NestedField> columns);

  /** truncates the table */
  void truncateTable();

  /**
   * adds new columns
   *
   * @param columnsToAdd list of columns fields to add
   */
  void addColumns(List<Types.NestedField> columnsToAdd);

  /**
   * drop an existing column
   *
   * @param columnToDrop existing column name
   */
  void dropColumn(String columnToDrop);

  /**
   * change column type
   *
   * @param columnToChange existing column name
   * @param batchField new column type
   */
  void changeColumn(String columnToChange, Field batchField);

  /**
   * replace the table sort order with a newly created order.
   *
   * @param sortOrder the names of columns in the new sort order. Columns are sorted from high
   *     priority to low priority
   */
  void replaceSortOrder(List<String> sortOrder);

  /**
   * change column name
   *
   * @param name existing column name
   * @param newName new column name
   */
  void renameColumn(String name, String newName);

  /**
   * Update primary key
   *
   * @param columns primary key column fields
   */
  void updatePrimaryKey(List<Field> columns);

  /** Update table's properties in a transaction */
  void updatePropertiesInTransaction(Map<String, String> tblProperties);

  /** Update table's properties. */
  void updateProperties(Map<String, String> tblProperties);

  /** remove table's properties. */
  void removeProperties(List<String> tblProperties);

  /**
   * Load an Iceberg table from disk
   *
   * @return Iceberg table instance
   */
  Table loadTable();

  /**
   * @return return TableOperations instance
   */
  TableOperations getTableOps();

  /**
   * @return returns the latest snapshot on which the transaction is performed
   */
  Snapshot getCurrentSnapshot();

  /**
   * @return return Iceberg table metadata file location
   */
  String getRootPointer();

  /** Delete the root pointer of the table */
  void deleteTableRootPointer();

  void deleteTable();

  Map<Integer, PartitionSpec> getPartitionSpecMap();

  Schema getIcebergSchema();

  void beginAlterTableTransaction();

  Table endAlterTableTransaction();

  void addColumnsInternalTable(List<Field> columnsToAdd);

  void dropColumnInternalTable(String columnToDrop);

  void changeColumnForInternalTable(String columnToChange, Field batchField);

  void updatePartitionSpec(PartitionSpecAlterOption partitionSpecAlterOption);

  long propertyAsLong(String propertyName, long defaultValue);

  FileIO getFileIO();
}
