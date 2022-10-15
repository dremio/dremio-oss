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
package com.dremio.exec.store.iceberg.deletes;

import static com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader.FILE_PATH_COLUMN;
import static com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader.POS_COLUMN;
import static com.dremio.exec.store.iceberg.deletes.PositionalDeleteFileReader.SCHEMA;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.concurrent.NotThreadSafe;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.parquet.AllRowGroupsParquetReader;
import com.dremio.exec.store.parquet.InputStreamProviderFactory;
import com.dremio.exec.store.parquet.ParquetFilterCondition;
import com.dremio.exec.store.parquet.ParquetFilters;
import com.dremio.exec.store.parquet.ParquetReaderFactory;
import com.dremio.exec.store.parquet.ParquetReaderOptions;
import com.dremio.exec.store.parquet.ParquetScanProjectedColumns;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Factory for creating Parquet-based {@link PositionalDeleteFileReader} and {@link EqualityDeleteFileReader} instances.
 */
@NotThreadSafe
public class ParquetRowLevelDeleteFileReaderFactory implements RowLevelDeleteFileReaderFactory {

  private final InputStreamProviderFactory inputStreamProviderFactory;
  private final ParquetReaderFactory parquetReaderFactory;
  private final FileSystem fs;
  private final List<String> dataset;
  private final BatchSchema tableSchema;

  public ParquetRowLevelDeleteFileReaderFactory(
      InputStreamProviderFactory inputStreamProviderFactory,
      ParquetReaderFactory parquetReaderFactory,
      FileSystem fs,
      List<String> dataset,
      BatchSchema tableSchema) {
    this.inputStreamProviderFactory = Preconditions.checkNotNull(inputStreamProviderFactory);
    this.parquetReaderFactory = Preconditions.checkNotNull(parquetReaderFactory);
    this.fs = Preconditions.checkNotNull(fs);
    this.dataset = dataset;
    this.tableSchema = tableSchema;
  }

  @Override
  public PositionalDeleteFileReader createPositionalDeleteFileReader(OperatorContext context, Path deleteFilePath,
      List<String> dataFilePaths) {
    Preconditions.checkArgument(!dataFilePaths.isEmpty(), "Data file paths cannot be empty.");
    List<String> sortedPaths = dataFilePaths.stream().sorted().collect(Collectors.toList());

    ParquetScanProjectedColumns projectedColumns = ParquetScanProjectedColumns.fromSchemaPaths(
        ImmutableList.of(SchemaPath.getSimplePath(FILE_PATH_COLUMN), SchemaPath.getSimplePath(POS_COLUMN)));
    List<ParquetFilterCondition> filterConditions = parquetReaderFactory.newDeleteFileFilterCreator()
        .createFilePathFilter(sortedPaths.get(0), sortedPaths.get(sortedPaths.size() - 1));

    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
        context,
        deleteFilePath,
        dataset,
        fs,
        inputStreamProviderFactory,
        parquetReaderFactory,
        SCHEMA,
        projectedColumns,
        new ParquetFilters(filterConditions),
        ParquetReaderOptions.from(context.getOptions()));

    return new PositionalDeleteFileReader(context, reader, sortedPaths.size());
  }

  @Override
  public EqualityDeleteFileReader createEqualityDeleteFileReader(OperatorContext context, Path deleteFilePath,
      long recordCount, List<Integer> equalityIds, List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds) {
    ParquetScanProjectedColumns projectedColumns = getProjectedColumnsFromEqualityIds(context, deleteFilePath,
        equalityIds, icebergColumnIds, tableSchema);

    AllRowGroupsParquetReader reader = new AllRowGroupsParquetReader(
        context,
        deleteFilePath,
        dataset,
        fs,
        inputStreamProviderFactory,
        parquetReaderFactory,
        tableSchema,
        projectedColumns,
        new ParquetFilters(),
        ParquetReaderOptions.from(context.getOptions()));

    return new EqualityDeleteFileReader(context, reader, tableSchema,
        projectedColumns.getBatchSchemaProjectedColumns(), recordCount);
  }

  private ParquetScanProjectedColumns getProjectedColumnsFromEqualityIds(OperatorContext context,
      Path deleteFilePath, List<Integer> equalityIds, List<IcebergProtobuf.IcebergSchemaField> icebergColumnIds,
      BatchSchema tableSchema) {
    // TODO: make this work with nested fields.. does icebergColumnIds even have the nested field info?
    List<SchemaPath> columns = equalityIds.stream()
        .map(id -> icebergColumnIds.stream().filter(col -> col.getId() == id).findFirst())
        .filter(Optional::isPresent)
        .map(col -> SchemaPath.getSimplePath(col.get().getSchemaPath()))
        .collect(Collectors.toList());

    if (columns.size() != equalityIds.size()) {
      throw new IllegalStateException(String.format(
         "Iceberg equality field ids specified for equality delete file not found in schema.\n" +
         "Path: %s\n" +
         "Iceberg table fields ([id] name): %s\n" +
         "Equality ids: %s\n",
         deleteFilePath.toString(),
         icebergColumnIds.stream()
             .map(col -> String.format("[%d] %s", col.getId(), col.getSchemaPath()))
             .collect(Collectors.joining(", ")),
          equalityIds.stream().map(Object::toString).collect(Collectors.joining(", "))));
    }

    return ParquetScanProjectedColumns.fromSchemaPathAndIcebergSchema(columns, icebergColumnIds, false, context,
        tableSchema);
  }
}
