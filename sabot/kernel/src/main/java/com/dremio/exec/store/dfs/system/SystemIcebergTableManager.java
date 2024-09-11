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

import com.dremio.common.expression.CompleteType;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.iceberg.IcebergPartitionData;
import com.dremio.exec.store.iceberg.IcebergUtils;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import org.apache.arrow.vector.ValueVector;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages the copy_jobs_history and copy_file_history tables used in copy-into
 * operation. It provides methods to initialize the table, write records to it, and handle data
 * files in the table.
 */
public class SystemIcebergTableManager {

  private static final Logger logger = LoggerFactory.getLogger(SystemIcebergTableManager.class);
  private static final String DATA_FILE_PREFIX = "data_";
  private static final String DATA_FILE_DIR = "/data/";
  private static final String DATA_FILE_EXTENSION = ".parquet";
  private final SystemIcebergTablesStoragePlugin plugin;
  private final OperatorContext context;
  private final SystemIcebergTableMetadata tableMetadata;
  private final Table table;
  private AppendFiles appendFiles;

  /**
   * Constructs an instance of SystemIcebergTableManager associated with the given plugin.
   *
   * @param context
   * @param plugin The SystemIcebergTablesStoragePlugin associated with this table manager.
   * @param tableMetadata
   */
  public SystemIcebergTableManager(
      OperatorContext context,
      SystemIcebergTablesStoragePlugin plugin,
      SystemIcebergTableMetadata tableMetadata) {
    this.plugin = plugin;
    this.context = context;
    this.tableMetadata = tableMetadata;
    this.table =
        Preconditions.checkNotNull(
            plugin.getTable(tableMetadata.getTableLocation()),
            "Failed to load table %s",
            tableMetadata.getTableName());
  }

  @VisibleForTesting
  public void setAppendFiles(AppendFiles appendFiles) {
    this.appendFiles = appendFiles;
  }

  /**
   * Write records to the system iceberg table.
   *
   * @param container The vector container object to be included in the data file.
   * @param partitionValueVectors List of vectors that contain the partition transformed values.
   * @throws IOException If an I/O error occurs while writing the records.
   */
  public void writeRecords(VectorContainer container, List<ValueVector> partitionValueVectors)
      throws Exception {
    if (appendFiles == null) {
      appendFiles = table.newAppend();
    }
    appendFiles.appendFile(getDataFile(container, partitionValueVectors));
    logger.debug(
        "Data file appended to {} system iceberg table at location {}",
        table.name(),
        table.location());
  }

  public void commit() {
    if (appendFiles != null) {
      IcebergUtils.stampSnapshotUpdateWithDremioJobId(
          appendFiles, QueryIdHelper.getQueryId(context.getFragmentHandle().getQueryId()));
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
   * @param partitionValueVectors List of vectors that contain the partition transformed values.
   * @return The DataFile representing the data file with the given record.
   * @throws IOException If an I/O error occurs while creating the data file.
   */
  private DataFile getDataFile(VectorContainer container, List<ValueVector> partitionValueVectors)
      throws Exception {
    FileSystem fs = plugin.getSystemUserFS();
    Path dataFilePath = getDataFilePath(fs);
    SystemIcebergTableRecordWriter recordWriter =
        new SystemIcebergTableRecordWriter(context, plugin, tableMetadata, getDataDirPath(fs));
    IcebergPartitionData icebergPartitionData =
        getIcebergPartitionData(tableMetadata.getPartitionSpec(), partitionValueVectors);

    logger.debug("Writing copy into error data to file at {}", dataFilePath);
    try {
      recordWriter.write(container, dataFilePath);
    } finally {
      recordWriter.close();
    }

    return new DataFiles.Builder(tableMetadata.getPartitionSpec())
        .withFormat(FileFormat.PARQUET)
        .withSortOrder(SortOrder.unsorted())
        .withPath(dataFilePath.toString())
        .withRecordCount(container.getRecordCount())
        .withFileSizeInBytes(recordWriter.getFileSize())
        .withMetrics(recordWriter.getIcebergMetrics())
        .withPartition(icebergPartitionData)
        .build();
  }

  /**
   * Builds the Iceberg partition data from the provided partition specification and partition value
   * vectors. For each partition spec field retrieves the appropriate partition value vector from
   * the given input list and sets it to the created partition data object.
   *
   * @param spec The Iceberg partition specification.
   * @param partitionValueVectors The list of partition value vectors.
   * @return The Iceberg partition data.
   */
  private IcebergPartitionData getIcebergPartitionData(
      PartitionSpec spec, List<ValueVector> partitionValueVectors) {
    IcebergPartitionData icebergPartitionData =
        new IcebergPartitionData(tableMetadata.partitionSpec.partitionType());
    for (int i = 0; i < spec.fields().size(); i++) {
      ValueVector valueVector = partitionValueVectors.get(i);
      icebergPartitionData.set(
          i, new CompleteType(valueVector.getMinorType().getType()), valueVector, 0);
    }
    return icebergPartitionData;
  }

  /**
   * Return a path pointing to the parent directory of the data files.
   *
   * @param fs The FileSystem instance.
   * @return The path to the data files parent directory.
   */
  private Path getDataDirPath(FileSystem fs) {
    Path path =
        plugin.resolveTablePathToValidPath(
            SystemIcebergTablesStoragePlugin.getTableName(tableMetadata.getNamespaceKey())
                + DATA_FILE_DIR);
    return fs.supportsPathsWithScheme()
        ? Path.of(Path.getContainerSpecificRelativePath(path))
        : path;
  }

  /**
   * Generates a data file path based on the provided FileSystem instance.
   *
   * @param fs The FileSystem instance.
   * @return The generated path for the data file.
   */
  private Path getDataFilePath(FileSystem fs) {
    return Path.of(
        getDataDirPath(fs)
            + DATA_FILE_PREFIX
            + System.currentTimeMillis()
            + UUID.randomUUID()
            + DATA_FILE_EXTENSION);
  }
}
