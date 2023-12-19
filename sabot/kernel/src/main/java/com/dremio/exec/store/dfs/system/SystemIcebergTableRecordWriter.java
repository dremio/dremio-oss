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

import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.physical.base.IcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableIcebergWriterOptions;
import com.dremio.exec.physical.base.ImmutableTableFormatWriterOptions;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.TableFormatWriterOptions;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.sql.parser.PartitionDistributionStrategy;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.dremio.exec.store.parquet.ParquetRecordWriter;
import com.dremio.exec.store.parquet.ParquetWriter;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.service.users.SystemUser;
import com.google.common.collect.ImmutableList;

/**
 * *** IMPORTANT ***
 * This record writer must be used only in the context of copy into command.
 * *****************
 * This class is responsible for writing records to the "system iceberg tables" in Parquet format.
 * It encapsulates the logic of setting up the {@link ParquetRecordWriter}, initializing it, writing records, and closing it.
 */
public class SystemIcebergTableRecordWriter {

  private final ParquetRecordWriter recordWriter;

  /**
   * Constructs a new instance of CopyIntoErrorsTableRecordWriter.
   *
   * @param operatorContext            The operator context associated with the operation.
   * @param plugin                     The SystemIcebergTablesStoragePlugin instance.
   * @param dataLocationPath           The location path for writing the data.
   */
  public SystemIcebergTableRecordWriter(OperatorContext operatorContext, SystemIcebergTablesStoragePlugin plugin, SystemIcebergTableMetadata tableMetadata, Path dataLocationPath) {
    this.recordWriter = new ParquetRecordWriter(operatorContext, getWriter(plugin, dataLocationPath.toString(),
      tableMetadata.getIcebergTablePropsForInsert()), getFormatConfig());
  }

  /**
   * Writes the contents of a {@link VectorContainer} to a Parquet data file. After the write operation the provided
   * {@link VectorContainer} is cleared and closed.
   *
   * @param container           The {@link VectorContainer} containing the data to be written.
   * @param dataFileLocationPath The path where the Parquet data file will be created.
   * @throws Exception If an error occurs during the writing process.
   */
  public void write(VectorContainer container, Path dataFileLocationPath) throws Exception {
    try {
      recordWriter.setup(container, null, null);
      recordWriter.startPartition(WritePartition.NONE);
      recordWriter.initRecordWriter(dataFileLocationPath);
      recordWriter.writeBatch(0, container.getRecordCount());
    } finally {
      container.clear();
      container.close();
    }
  }

  /**
   * Close the underlying {@link ParquetRecordWriter} instance.
   * @throws Exception if an error occurs during the closing process.
   */
  public void close() throws Exception {
    recordWriter.close();
  }

  /**
   * Get the file size of the written parquet file.
   * @return File size, always non-null.
   */
  public long getFileSize() {
    return recordWriter.getFileSize();
  }

  /**
   * Get a ParquetWriter instance for writing Parquet data.
   *
   * @param plugin            The MutablePlugin instance associated with the writer.
   * @param dataLocation      The location where the data will be written.
   * @param icebergTableProps
   * @return A ParquetWriter instance for writing data.
   */
  private ParquetWriter getWriter(MutablePlugin plugin, String dataLocation, IcebergTableProps icebergTableProps) {
    return new ParquetWriter(getOpProps(), null, dataLocation, getWriterOptions(icebergTableProps), plugin);
  }

  /**
   * Generates the {@link WriterOptions} for writing data to an Iceberg table.
   *
   * @param icebergTableProps The {@link IcebergTableProps} containing properties specific to the Iceberg table.
   * @return The {@link WriterOptions} used for writing data to the table.
   */
  private WriterOptions getWriterOptions(IcebergTableProps icebergTableProps) {
    return new WriterOptions(null, ImmutableList.of(), ImmutableList.<String>of(), ImmutableList.<String>of(),
      PartitionDistributionStrategy.UNSPECIFIED, null, false,
      Long.MAX_VALUE, getTableFormatWriterOptions(icebergTableProps), null);
  }

  /**
   * Generates the {@link TableFormatWriterOptions} for writing data to an Iceberg table in a specific format.
   *
   * @param icebergTableProps The {@link IcebergTableProps} containing properties specific to the Iceberg table.
   * @return The {@link TableFormatWriterOptions} used for writing data to the table.
   */
  private TableFormatWriterOptions getTableFormatWriterOptions(IcebergTableProps icebergTableProps) {
    return new ImmutableTableFormatWriterOptions.Builder()
      .setOperation(TableFormatWriterOptions.TableFormatOperation.INSERT)
      .setIcebergSpecificOptions(getIcebergWriterOptions(icebergTableProps))
      .build();
  }

  /**
   * Generates the {@link IcebergWriterOptions} for writing data to an Iceberg table with specific properties.
   *
   * @param icebergTableProps The {@link IcebergTableProps} containing properties specific to the Iceberg table.
   * @return The {@link IcebergWriterOptions} used for writing data to the table.
   */
  private IcebergWriterOptions getIcebergWriterOptions(IcebergTableProps icebergTableProps) {
    return new ImmutableIcebergWriterOptions.Builder().setIcebergTableProps(icebergTableProps).build();
  }

  /**
   * Get a ParquetFormatConfig instance for configuring the Parquet format.
   *
   * @return A ParquetFormatConfig instance.
   */
  private ParquetFormatConfig getFormatConfig() {
    return new ParquetFormatConfig();
  }

  /**
   * Get an OpProps instance for configuring the operation properties.
   *
   * @return An OpProps instance for operation properties.
   */
  private OpProps getOpProps() {
    return new OpProps(0, SystemUser.SYSTEM_USERNAME, 0, Long.MAX_VALUE, 0, 0, false, 4095, RecordWriter.SCHEMA, false, 0.0d, false);
  }

}
