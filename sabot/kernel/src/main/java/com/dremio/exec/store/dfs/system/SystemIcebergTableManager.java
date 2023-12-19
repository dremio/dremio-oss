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
package com.dremio.exec.store.dfs.system;

import java.io.IOException;
import java.util.UUID;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.record.VectorContainer;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;

/**
 * This class manages the copy_jobs_history and copy_file_history tables used in copy-into operation.
 * It provides methods to initialize the table, write records to it, and handle data files in the table.
 **/
public class SystemIcebergTableManager {

  private static final Logger logger = LoggerFactory.getLogger(SystemIcebergTableManager.class);
  private static final String DATA_FILE_PREFIX = "data_";
  private static final String DATA_FILE_DIR = "/data/";
  private static final String DATA_FILE_EXTENSION = ".parquet";
  private final SystemIcebergTablesStoragePlugin plugin;
  private final OperatorContext context;
  private final SystemIcebergTableMetadata tableMetadata;
  private Table table;
  private AppendFiles appendFiles;

  /**
   * Constructs an instance of SystemIcebergTableManager associated with the given plugin.
   *
   * @param context
   * @param plugin The SystemIcebergTablesStoragePlugin associated with this table manager.
   * @param tableMetadata
   */
  public SystemIcebergTableManager(OperatorContext context, SystemIcebergTablesStoragePlugin plugin, SystemIcebergTableMetadata tableMetadata) {
    this.plugin = plugin;
    this.context = context;
    this.tableMetadata = tableMetadata;
    this.table = plugin.getTable(tableMetadata.getTableLocation());
  }

  /**
   * Write records to the system iceberg table.
   *
   * @param container The vector container object to be included in the data file.
   * @throws IOException If an I/O error occurs while writing the records.
   */
  public void writeRecords(VectorContainer container) throws Exception {
    if (appendFiles == null) {
      appendFiles = table.newAppend();
    }
    appendFiles.appendFile(getDataFile(container));
    logger.debug("Data file appended to {} system iceberg table at location {}", table.name(), table.location());
  }

  public void commit() {
    if (appendFiles != null) {
      appendFiles.commit();
      logger.debug("System iceberg table {} data files committed.", table.name());
    } else {
      logger.debug("Nothing to commit to {} system iceberg table", table.name());
    }
  }

  /**
   * Get the DataFile representing the data file for the given record.
   *
   * @param container The vector container object to be included in the data file.
   * @return The DataFile representing the data file with the given record.
   * @throws IOException If an I/O error occurs while creating the data file.
   */
  private DataFile getDataFile(VectorContainer container) throws Exception {
    FileSystem fs = plugin.getSystemUserFS();
    Path dataFilePath = getDataFilePath(fs);
    long fileSize = writeDataFile(fs, container, dataFilePath);
    return new DataFiles.Builder(PartitionSpec.unpartitioned()).withFormat(FileFormat.PARQUET)
      .withSortOrder(SortOrder.unsorted()).withPath(dataFilePath.toString()).withRecordCount(container.getRecordCount())
      .withFileSizeInBytes(fileSize).build();
  }

/**
 * Writes the contents of a {@link VectorContainer} to a Parquet data file and returns the size of the written file.
 *
 * @param fs            The {@link FileSystem} instance used for writing the data file.
 * @param container     The {@link VectorContainer} containing the data to be written.
 * @param dataFilePath  The path where the Parquet data file will be created.
 * @return The size of the written Parquet file in bytes.
 * @throws Exception If an error occurs during the writing process.
 */
  private long writeDataFile(FileSystem fs, VectorContainer container, Path dataFilePath) throws Exception{
    SystemIcebergTableRecordWriter writer = new SystemIcebergTableRecordWriter(context, plugin, tableMetadata, getDataDirPath(fs));
    logger.debug("Writing copy into error data to file at {}", dataFilePath);
    try {
      writer.write(container, dataFilePath);
    } finally {
      writer.close();
    }
    return writer.getFileSize();
  }

  /**
   * Return a path pointing to the parent directory of the data files.
   *
   * @param fs The FileSystem instance.
   * @return The path to the data files parent directory.
   */
  private Path getDataDirPath(FileSystem fs) {
    Path path = plugin.resolveTablePathToValidPath(SystemIcebergTablesStoragePlugin.getTableName(tableMetadata.getNamespaceKey()) + DATA_FILE_DIR);
    return fs.supportsPathsWithScheme() ? Path.of(Path.getContainerSpecificRelativePath(path)) : path;
  }

  /**
   * Generates a data file path based on the provided FileSystem instance.
   *
   * @param fs The FileSystem instance.
   * @return The generated path for the data file.
   */
  private Path getDataFilePath(FileSystem fs) {
    return Path.of(getDataDirPath(fs) + DATA_FILE_PREFIX + System.currentTimeMillis() + UUID.randomUUID() + DATA_FILE_EXTENSION);
  }
}
