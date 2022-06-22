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
import java.util.stream.Collectors;

import com.dremio.common.expression.SchemaPath;
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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Factory for creating Parquet-based PositionalDeleteFileReader instances.
 */
public class ParquetPositionalDeleteFileReaderFactory implements PositionalDeleteFileReaderFactory {

  private final InputStreamProviderFactory inputStreamProviderFactory;
  private final ParquetReaderFactory parquetReaderFactory;
  private final FileSystem fs;
  private final List<String> dataset;

  public ParquetPositionalDeleteFileReaderFactory(
      InputStreamProviderFactory inputStreamProviderFactory,
      ParquetReaderFactory parquetReaderFactory,
      FileSystem fs,
      List<String> dataset) {
    this.inputStreamProviderFactory = Preconditions.checkNotNull(inputStreamProviderFactory);
    this.parquetReaderFactory = Preconditions.checkNotNull(parquetReaderFactory);
    this.fs = Preconditions.checkNotNull(fs);
    this.dataset = dataset;
  }

  @Override
  public PositionalDeleteFileReader create(OperatorContext context, Path deleteFilePath, List<String> dataFilePaths) {
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
}
