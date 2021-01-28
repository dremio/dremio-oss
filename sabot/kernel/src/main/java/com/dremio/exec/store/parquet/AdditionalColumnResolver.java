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
package com.dremio.exec.store.parquet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A wrapper around {@link ParquetColumnResolver} to resolve columns, taking into consideration any new columns
 * which are not present in the currently known table schema.
 */
public class AdditionalColumnResolver {
  private final BatchSchema tableSchema;
  private final ParquetColumnResolver columnResolver;

  public AdditionalColumnResolver(BatchSchema tableSchema, ParquetColumnResolver columnResolver) {
    this.tableSchema = tableSchema;
    this.columnResolver = columnResolver;
  }

  public Collection<SchemaPath> resolveColumns(List<ColumnChunkMetaData> metadata) {
    if (!ColumnUtils.isStarQuery(columnResolver.getBatchSchemaProjectedColumns())) {
      // Return all selected columns + any additional columns that are not present in the table schema (for schema
      // learning purpose)
      List<SchemaPath> columnsToRead = Lists.newArrayList(columnResolver.getProjectedParquetColumns());
      Set<String> columnsTableDef = Sets.newHashSet();
      Set<String> columnsTableDefLowercase = Sets.newHashSet();
      tableSchema.forEach(f -> columnsTableDef.add(f.getName()));
      tableSchema.forEach(f -> columnsTableDefLowercase.add(f.getName().toLowerCase()));
      for (ColumnChunkMetaData c : metadata) {
        final String columnInParquetFile = c.getPath().iterator().next();
        // Column names in parquet are case sensitive, in Dremio they are case insensitive
        // First try to find the column with exact case. If not found try the case insensitive comparision.
        String batchSchemaColumnName = columnResolver.getBatchSchemaColumnName(columnInParquetFile);
        if (batchSchemaColumnName != null &&
          !columnsTableDef.contains(batchSchemaColumnName) &&
          !columnsTableDefLowercase.contains(batchSchemaColumnName.toLowerCase())) {
          columnsToRead.add(SchemaPath.getSimplePath(columnInParquetFile));
        }
      }

      return columnsToRead;
    }

    List<SchemaPath> paths = new ArrayList<>();
    for (ColumnChunkMetaData c : metadata) {
      paths.add(SchemaPath.getSimplePath(c.getPath().iterator().next()));
    }

    return paths;
  }
}