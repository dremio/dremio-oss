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
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import com.dremio.exec.record.BatchSchema;

/**
 * represents an Iceberg catalog
 */
public interface IcebergCommand {
    /**
     * Start of Create table command
     * @param tableName name of the table
     * @param writerSchema schema of the table
     * @param partitionColumns partition specification of the table
     */
    void beginCreateTableTransaction(String tableName, BatchSchema writerSchema, List<String> partitionColumns);

    /**
     * End of Create command
     */
    void endCreateTableTransaction();

    /**
     * Start of Insert command
     */
    void beginInsertTableTransaction();

    /**
     * End of Insert command
     */
    void endInsertTableTransaction();

    /**
     * Start of MetadataRefresh
     */
    void beginMetadataRefreshTransaction();

    /**
     * End of MetadataRefresh
     */
    void endMetadataRefreshTransaction();

    /**
     * Commit the delete operation
     */
    void beginDelete();

    /**
     * Commit the delete operation
     */
    void finishDelete();

    /**
     *  Start the insert operation
     */
    void beginInsert();

    /**
     *  Finish the insert operation
     */
    void finishInsert();

    /**
     * consumes list of Manifest files as part of the current transaction
     * @param filesList list of DataFile entries
     */
    void consumeManifestFiles(List<ManifestFile> filesList);

    /**
     * consumes list of data files to be deleted as a part of
     * the current transaction
     * @param filesList list of DataFile entries
     */
    void consumeDeleteDataFiles(List<DataFile> filesList);

    /**
     * truncates the table
     */
    void truncateTable();

    /**
     * adds new columns
     * @param columnsToAdd list of columns fields to add
     */
    void addColumns(List<Types.NestedField> columnsToAdd);

    /**
     * drop an existing column
     * @param columnToDrop existing column name
     */
    void dropColumn(String columnToDrop);

    /**
     * change column type
     * @param columnToChange existing column name
     * @param batchField new column type
     */
    void changeColumn(String columnToChange, Field batchField);

    /**
     * change column name
     * @param name existing column name
     * @param newName new column name
     */
    void renameColumn(String name, String newName);

    /**
     * Load an Iceberg table from disk
     * @return Iceberg table instance
     */
    Table loadTable();
}
