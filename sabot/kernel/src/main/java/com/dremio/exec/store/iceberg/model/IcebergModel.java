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

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import com.dremio.exec.record.BatchSchema;

/**
 * This interface is the entry point to Iceberg tables
 */
public interface IcebergModel {
    IcebergOpCommitter getCreateTableCommitter(String tableName, IcebergTableIdentifier tableIdentifier,
                                               BatchSchema batchSchema,
                                               List<String> partitionColumnNames);

  /**
     * Get Iceberg Op committer for Insert command
     * @param tableIdentifier Table identifier
     * @return Insert committer
     */
    IcebergOpCommitter getInsertTableCommitter(IcebergTableIdentifier tableIdentifier);

  /**
   * Get committer for Full metadata refresh
   * @param tableName
   * @param tableLocation
   * @param tableIdentifier
   * @param batchSchema
   * @param partitionColumnNames
   * @return
   */
  IcebergOpCommitter getFullMetadataRefreshCommitter(String tableName, String tableLocation, IcebergTableIdentifier tableIdentifier,
                                                     BatchSchema batchSchema, List<String> partitionColumnNames);

  /**
   * Get Iceberg Op committer for Metadata Incremental Refresh command
   * @param tableName
   * @param tableLocation
   * @param tableIdentifier
   * @param batchSchema
   * @param partitionColumnNames
   * @return
   */
  IcebergOpCommitter getIncrementalMetadataRefreshCommitter(String tableName, String tableLocation, IcebergTableIdentifier tableIdentifier,
                                                            BatchSchema batchSchema, List<String> partitionColumnNames);

    /**
     * Truncate a table
     * @param tableIdentifier table identifier
     */
    void truncateTable(IcebergTableIdentifier tableIdentifier);

    /**
     * Add columns to a table
     * @param tableIdentifier table identifier
     * @param columnsToAdd list of columns to add
     */
    void addColumns(IcebergTableIdentifier tableIdentifier, List<Types.NestedField> columnsToAdd);

    /**
     * Drop a column from a table
     * @param tableIdentifier table identifier
     * @param columnToDrop Column name to drop
     */
    void dropColumn(IcebergTableIdentifier tableIdentifier, String columnToDrop);

    /**
     * Change column type of a table
     * @param tableIdentifier table identifier
     * @param columnToChange existing column name
     * @param newDef new type
     */
    void changeColumn(IcebergTableIdentifier tableIdentifier, String columnToChange, Field newDef);

    /**
     * Rename an existing column of a table
     * @param tableIdentifier table identifier
     * @param name existing column name
     * @param newName new column name
     */
    void renameColumn(IcebergTableIdentifier tableIdentifier, String name, String newName);

    /**
     * Load and return an Iceberg table
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
}
