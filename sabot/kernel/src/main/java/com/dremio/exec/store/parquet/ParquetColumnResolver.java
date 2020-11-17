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

import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.SchemaPath;
/**
 * Interface to resolve column names during parquet scan operation
 */
public interface ParquetColumnResolver {

  /**
   *
   * @return project column schemapaths from table schema
   */
  List<SchemaPath> getBatchSchemaProjectedColumns();

  /**
   * @return returns projected column names from parquet schema
   */
  List<SchemaPath> getProjectedParquetColumns();

  /**
   *
   * @param columnInParquetFile Column name form parquet schema
   * @return column name from table schema
   */
  String getBatchSchemaColumnName(String columnInParquetFile);

  /**
   *
   * @param columnInParquetFile list of column names from parquet schema
   * @return list of column names from table schema
   */
  List<String> getBatchSchemaColumnName(List<String> columnInParquetFile);

  /**
   *
   * @param name Column name from table schema
   * @return column name from parquet schema
   */
  String getParquetColumnName(String name);

  /**
   *
   * @param parquestSchemaPaths list of schemapaths from parquet schema
   * @return list of schemapaths from table schema
   */
  List<SchemaPath> getBatchSchemaColumns(List<SchemaPath> parquestSchemaPaths);

  /**
   *
   * @param pathInParquetFile schema path in parquet file
   * @return schema path from batch schema
   */
  SchemaPath getBatchSchemaColumnPath(SchemaPath pathInParquetFile);

  /**
   *
   * @param schemaPath input schema path
   * @return list of name segments from the path
   */
  List<String> getNameSegments(SchemaPath schemaPath);

  /**
   *
   * @param schema
   * @param columnDesc
   * @return
   */
  List<String> convertColumnDescriptor(MessageType schema, ColumnDescriptor columnDesc);

  /**
   *
   * @param schemaPath
   * @param vector
   * @return Dotted string representation of given schemaPath
   */
  String toDotString(SchemaPath schemaPath, ValueVector vector);
}
