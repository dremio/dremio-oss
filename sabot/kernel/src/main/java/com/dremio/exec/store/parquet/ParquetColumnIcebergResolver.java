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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ValueVector;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.collect.Lists;

/**
 *  This class maps column names from batch schema to parquet schema and vice versa for Iceberg datasets.
 *  It maintains
 *   a) column name <=> ID mapping from Iceberg schema
 *   b) column name <=> ID mapping from parquet file schema
 */
public class ParquetColumnIcebergResolver implements ParquetColumnResolver {
  private final List<SchemaPath> projectedColumns;
  private CaseInsensitiveImmutableBiMap<Integer> icebergColumnIDMap;
  private CaseInsensitiveImmutableBiMap<Integer> parquetColumnIDs;

  public ParquetColumnIcebergResolver(List<SchemaPath> projectedColumns,
                               List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs,
                               Map<String, Integer> parquetColumnIDs) {
    this.projectedColumns = projectedColumns;
    this.parquetColumnIDs = CaseInsensitiveImmutableBiMap.newImmutableMap(parquetColumnIDs);
    initializeProjectedColumnIDs(icebergColumnIDs);
  }


  private void initializeProjectedColumnIDs(List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs) {
    Map<String, Integer> icebergColumns = new HashMap<>();
    icebergColumnIDs.forEach(field -> icebergColumns.put(field.getSchemaPath(), field.getId()));
    this.icebergColumnIDMap = CaseInsensitiveImmutableBiMap.newImmutableMap(icebergColumns);
  }

  public List<SchemaPath> getBatchSchemaProjectedColumns() {
    return projectedColumns;
  }

  public List<SchemaPath> getProjectedParquetColumns() {
    return this.projectedColumns.stream()
      .map(this::getParquetColumnPath)
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  public String getBatchSchemaColumnName(String columnInParquetFile) {
    if (!this.parquetColumnIDs.containsKey(columnInParquetFile)) {
      return null;
    }

    int id = this.parquetColumnIDs.get(columnInParquetFile);

    if (!this.icebergColumnIDMap.containsValue(id)) {
      return null;
    }

    return this.icebergColumnIDMap.inverse().get(id);
  }

  public List<String> getBatchSchemaColumnName(List<String> columnInParquetFile) {
    String columnName = String.join(".", columnInParquetFile);

    if (!this.parquetColumnIDs.containsKey(columnName)) {
      return null;
    }

    int id = this.parquetColumnIDs.get(columnName);

    if (!this.icebergColumnIDMap.containsValue(id)) {
      return null;
    }

    String columnInSchema = this.icebergColumnIDMap.inverse().get(id);
    return Lists.newArrayList(columnInSchema.split("\\."));
  }

  public String getParquetColumnName(String name) {
    if (!this.icebergColumnIDMap.containsKey(name)) {
      return null;
    }

    Integer id = this.icebergColumnIDMap.get(name);

    if (!this.parquetColumnIDs.containsValue(id)) {
      return null;
    }

    return this.parquetColumnIDs.inverse().get(id);
  }

  public List<SchemaPath> getBatchSchemaColumns(List<SchemaPath> parquestSchemaPaths) {
    return parquestSchemaPaths.stream()
      .map(this::getBatchSchemaColumnPath)
      .filter(Objects::nonNull)
      .collect(Collectors.toList());
  }

  public SchemaPath getBatchSchemaColumnPath(SchemaPath pathInParquetFile) {
    List<String> pathSegmentsInParquet = pathInParquetFile.getComplexNameSegments();
    List<String> pathSegmentsInBatchSchema = getBatchSchemaColumnName(pathSegmentsInParquet);

    return (pathSegmentsInBatchSchema == null) ? null :
      SchemaPath.getCompoundPath(pathSegmentsInBatchSchema.toArray(new String[0]));
  }

  public List<String> getNameSegments(SchemaPath schemaPath) {
    return schemaPath.getComplexNameSegments();
  }

  private SchemaPath getParquetColumnPath(SchemaPath pathInBatchSchema) {
    List<String> pathSegmentsInBatchSchema = pathInBatchSchema.getComplexNameSegments();
    List<String> pathSegmentsInParquet = getParquetSchemaColumnName(pathSegmentsInBatchSchema);
    return pathSegmentsInParquet == null ? null :
      SchemaPath.getCompoundPath(pathSegmentsInParquet.toArray(new String[0]));
  }

  private List<String> getParquetSchemaColumnName(List<String> columnInBatchSchema) {
    String columnName = String.join(".", columnInBatchSchema);

    if (!this.icebergColumnIDMap.containsKey(columnName)) {
      return null;
    }

    int id = this.icebergColumnIDMap.get(columnName);

    if (!this.parquetColumnIDs.containsValue(id)) {
      return null;
    }

    String columnInParquet = this.parquetColumnIDs.inverse().get(id);
    return Lists.newArrayList(columnInParquet.split("\\."));
  }

  public List<String> convertColumnDescriptor(MessageType schema, ColumnDescriptor columnDescriptor) {
    return Lists.newArrayList(columnDescriptor.getPath());
  }

  public String toDotString(SchemaPath schemaPath, ValueVector vector) {
    return schemaPath.toDotString().toLowerCase();
  }
}
