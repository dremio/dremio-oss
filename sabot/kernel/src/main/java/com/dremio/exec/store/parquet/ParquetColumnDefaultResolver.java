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

import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.SchemaPath;

/**
 * This class acts as no-op. This is used in parquet dataset path
 */
public class ParquetColumnDefaultResolver implements ParquetColumnResolver {
  private List<SchemaPath> projectedColumns;

  ParquetColumnDefaultResolver(List<SchemaPath> projectedColumns) {
    this.projectedColumns = projectedColumns;
  }

  public List<SchemaPath> getBatchSchemaProjectedColumns() {
    return projectedColumns;
  }

  public List<SchemaPath> getProjectedParquetColumns() {
    return projectedColumns;
  }

  public String getBatchSchemaColumnName(String columnInParquetFile) {
    return columnInParquetFile;
  }

  public List<String> getBatchSchemaColumnName(List<String> columnInParquetFile) {
    return columnInParquetFile;
  }

  public String getParquetColumnName(String name) {
    return name;
  }

  public List<SchemaPath> getBatchSchemaColumns(List<SchemaPath> parquestSchemaPaths) {
    return parquestSchemaPaths;
  }

  public SchemaPath getBatchSchemaColumnPath(SchemaPath pathInParquetFile) {
    return pathInParquetFile;
  }

  public List<String> getNameSegments(SchemaPath schemaPath) {
    return schemaPath.getNameSegments();
  }

  public List<String> convertColumnDescriptor(MessageType schema, ColumnDescriptor columnDesc) {
    return ParquetReaderUtility.convertColumnDescriptor(schema, columnDesc);
  }
}
