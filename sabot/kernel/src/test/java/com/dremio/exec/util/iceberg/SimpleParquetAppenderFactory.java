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

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.lang3.function.TriConsumer;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.OutputFile;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import com.google.common.base.Preconditions;

/**
 * Factory for creating parquet writers for data, position delete,
 * and equality delete files.
 */
public class SimpleParquetAppenderFactory implements FileAppenderFactory<StructLike> {

  private final Schema schema;
  private final PartitionSpec spec;
  private final Schema positionDeleteSchema;
  private final Schema equalityDeleteSchema;
  private final List<Integer> equalityFieldIds;
  private final Function<Schema, MessageType> schemaConverter;
  private final TriConsumer<Schema, RecordConsumer, StructLike> recordWriter;

  public SimpleParquetAppenderFactory(
    final Table table,
    final Schema positionDeleteSchema,
    final Schema equalityDeleteSchema,
    final List<Integer> equalityFieldIds,
    final Function<Schema, MessageType> schemaConverter,
    final TriConsumer<Schema, RecordConsumer, StructLike> recordWriter
  ) {
    this.schema = table.schema();
    this.spec = table.spec();
    this.positionDeleteSchema = positionDeleteSchema;
    this.equalityDeleteSchema = equalityDeleteSchema;
    this.equalityFieldIds = equalityFieldIds;
    this.schemaConverter = schemaConverter;
    this.recordWriter = recordWriter;
  }

  @Override
  public FileAppender<StructLike> newAppender(OutputFile outputFile, FileFormat format) {
    Preconditions.checkArgument(format == PARQUET);
    try {
      final ParquetWriter<StructLike> writer = new ParquetWriter<>(
        new Path(outputFile.location()),
        new IcebergParquetRecordWriter<>(schema, schemaConverter, recordWriter));
      return new SimpleParquetAppender<>(writer, outputFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public DataWriter<StructLike> newDataWriter(
    EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    return new org.apache.iceberg.io.DataWriter<>(
      newAppender(outputFile.encryptingOutputFile(), format),
      format,
      outputFile.encryptingOutputFile().location(),
      spec,
      partition,
      outputFile.keyMetadata());
  }

  @Override
  public EqualityDeleteWriter<StructLike> newEqDeleteWriter(
    EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    Preconditions.checkArgument(format == PARQUET);
    Preconditions.checkNotNull(equalityDeleteSchema);
    Preconditions.checkNotNull(equalityFieldIds);
    try {
      final ParquetWriter<StructLike> writer = new ParquetWriter<>(
        new Path(outputFile.encryptingOutputFile().location()),
        new IcebergParquetRecordWriter<>(
          equalityDeleteSchema, schemaConverter, recordWriter));
      return new EqualityDeleteWriter<>(
        new SimpleParquetAppender<>(writer, outputFile.encryptingOutputFile()),
        format,
        outputFile.encryptingOutputFile().location(),
        spec,
        partition,
        outputFile.keyMetadata(),
        null,
        equalityFieldIds.stream().mapToInt(Integer::intValue).toArray());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public PositionDeleteWriter<StructLike> newPosDeleteWriter(
    EncryptedOutputFile outputFile, FileFormat format, StructLike partition) {
    Preconditions.checkArgument(format == PARQUET);
    Preconditions.checkNotNull(positionDeleteSchema);
    try {
      final ParquetWriter<StructLike> writer = new ParquetWriter<>(
        new Path(outputFile.encryptingOutputFile().location()),
        new IcebergParquetRecordWriter<>(
          positionDeleteSchema, schemaConverter, recordWriter));
      return new PositionDeleteWriter<>(
        new SimpleParquetAppender<>(writer, outputFile.encryptingOutputFile()),
        format,
        outputFile.encryptingOutputFile().location(),
        spec,
        partition,
        outputFile.keyMetadata());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
