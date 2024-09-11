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
package com.dremio.exec.util.iceberg;

import static org.apache.iceberg.FileFormat.PARQUET;

import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.DeleteSchemaUtil;
import org.apache.iceberg.io.OutputFileFactory;

/** Utilities for writing data/delete files in parquet format for Iceberg tables */
public class ParquetFileWriterUtils {

  /**
   * Writes a parquet data file to an Iceberg table
   *
   * @param table The Iceberg table
   * @param partitionKey The partition key that the data belongs to
   * @param partitionId First part of generated filename
   * @param taskId Second part of generated filename
   * @param records The record data to write
   * @return The written data file
   */
  public static DataFile writeDataFile(
      final Table table,
      final StructLike partitionKey,
      final int partitionId,
      final long taskId,
      final Iterable<StructLike> records)
      throws Exception {
    if (partitionKey == null) {
      Preconditions.checkArgument(table.spec().isUnpartitioned());
    }

    final OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId).format(PARQUET).build();
    EncryptedOutputFile file =
        partitionKey == null
            ? fileFactory.newOutputFile()
            : fileFactory.newOutputFile(partitionKey);
    final SimpleParquetAppenderFactory appenderFactory =
        new SimpleParquetAppenderFactory(
            table,
            DeleteSchemaUtil.pathPosSchema(),
            null,
            null,
            IcebergParquetRecordWriter::convertPrimitiveSchema,
            IcebergParquetRecordWriter::writePrimitiveRecord);
    final DataWriter<StructLike> writer =
        appenderFactory.newDataWriter(file, PARQUET, partitionKey);
    try (final DataWriter<StructLike> w = writer) {
      records.forEach(w::write);
    }
    return writer.toDataFile();
  }

  /**
   * Writes a parquet data file of non-partitioned data to an Iceberg table
   *
   * @param table The Iceberg table
   * @param records The record data to write
   * @return The written data file
   */
  public static DataFile writeDataFile(final Table table, final Iterable<StructLike> records)
      throws Exception {
    return writeDataFile(table, null, 0, 0, records);
  }

  /**
   * Writes a parquet position delete file to an Iceberg table
   *
   * @param table The Iceberg table
   * @param partitionKey The partition key that the data belongs to
   * @param partitionId First part of generated filename
   * @param taskId Second part of generated filename
   * @param records The position delete records to write
   * @return The written delete file
   */
  public static DeleteFile writePositionDeleteFile(
      final Table table,
      final StructLike partitionKey,
      final int partitionId,
      final long taskId,
      final Iterable<PositionDelete<StructLike>> records)
      throws Exception {
    if (partitionKey == null) {
      Preconditions.checkArgument(table.spec().isUnpartitioned());
    }

    final OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(PARQUET)
            .suffix("deletes")
            .build();
    EncryptedOutputFile file =
        partitionKey == null
            ? fileFactory.newOutputFile()
            : fileFactory.newOutputFile(partitionKey);
    final SimpleParquetAppenderFactory appenderFactory =
        new SimpleParquetAppenderFactory(
            table,
            DeleteSchemaUtil.pathPosSchema(),
            null,
            null,
            IcebergParquetRecordWriter::convertPrimitiveSchema,
            IcebergParquetRecordWriter::writePrimitiveRecord);
    final PositionDeleteWriter<StructLike> writer =
        appenderFactory.newPosDeleteWriter(file, PARQUET, partitionKey);
    try (final PositionDeleteWriter<StructLike> w = writer) {
      records.forEach(w::write);
    }
    return writer.toDeleteFile();
  }

  /**
   * Writes a parquet position delete file for non-partitioned data to an Iceberg table
   *
   * @param table The Iceberg table
   * @param records The position delete records to write
   * @return The written delete file
   */
  public static DeleteFile writePositionDeleteFile(
      final Table table, final Iterable<PositionDelete<StructLike>> records) throws Exception {
    return writePositionDeleteFile(table, null, 0, 0, records);
  }

  /**
   * Writes a parquet equality delete file to an Iceberg table
   *
   * @param table The Iceberg table
   * @param partitionKey The partition key that the data belongs to
   * @param partitionId First part of generated filename
   * @param taskId Second part of generated filename
   * @param records The equality delete records to write
   * @return The written delete file
   */
  public static DeleteFile writeEqualityDeleteFile(
      final Table table,
      final StructLike partitionKey,
      final int partitionId,
      final long taskId,
      final Schema deleteSchema,
      final List<Integer> equalityDeleteIds,
      final Iterable<StructLike> records)
      throws Exception {
    if (partitionKey == null) {
      Preconditions.checkArgument(table.spec().isUnpartitioned());
    }

    final OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, partitionId, taskId)
            .format(PARQUET)
            .suffix("deletes")
            .build();
    EncryptedOutputFile file =
        partitionKey == null
            ? fileFactory.newOutputFile()
            : fileFactory.newOutputFile(partitionKey);
    final SimpleParquetAppenderFactory appenderFactory =
        new SimpleParquetAppenderFactory(
            table,
            null,
            deleteSchema,
            equalityDeleteIds,
            IcebergParquetRecordWriter::convertPrimitiveSchema,
            IcebergParquetRecordWriter::writePrimitiveRecord);
    final EqualityDeleteWriter<StructLike> writer =
        appenderFactory.newEqDeleteWriter(file, PARQUET, partitionKey);
    try (final EqualityDeleteWriter<StructLike> w = writer) {
      records.forEach(w::write);
    }
    return writer.toDeleteFile();
  }

  /**
   * Writes a parquet equality delete file for non-partitioned data to an Iceberg table
   *
   * @param table The Iceberg table
   * @param records The equality delete records to write
   * @return The written delete file
   */
  public static DeleteFile writeEqualityDeleteFile(
      final Table table,
      final Schema deleteSchema,
      final List<Integer> equalityDeleteIds,
      final Iterable<StructLike> records)
      throws Exception {
    return writeEqualityDeleteFile(table, null, 0, 0, deleteSchema, equalityDeleteIds, records);
  }
}
