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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveImmutableBiMap;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.exec.store.iceberg.proto.IcebergProtobuf;
import com.google.common.base.Preconditions;
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
  private Map<String, FieldInfo> fieldInfoMap = null;
  private final boolean shouldUseBatchSchemaForResolvingProjectedColumn;

  public ParquetColumnIcebergResolver(List<SchemaPath> projectedColumns,
                               List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs,
                               Map<String, Integer> parquetColumnIDs, boolean shouldUseBatchSchemaForResolvingProjectedColumn, BatchSchema batchSchema) {
    this.projectedColumns = projectedColumns;
    this.parquetColumnIDs = CaseInsensitiveImmutableBiMap.newImmutableMap(parquetColumnIDs);
    initializeProjectedColumnIDs(icebergColumnIDs);
    this.shouldUseBatchSchemaForResolvingProjectedColumn = shouldUseBatchSchemaForResolvingProjectedColumn;
    if(shouldUseBatchSchemaForResolvingProjectedColumn) {
      this.fieldInfoMap = getFieldsMapForBatchSchema(batchSchema);
    }
  }


  public ParquetColumnIcebergResolver(List<SchemaPath> projectedColumns,
                                      List<IcebergProtobuf.IcebergSchemaField> icebergColumnIDs,
                                      Map<String, Integer> parquetColumnIDs) {
    this(projectedColumns,icebergColumnIDs, parquetColumnIDs,false, null);
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

    if (columnInParquetFile.size() == 1) {
      return Lists.newArrayList(columnInSchema);
    }

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
    return shouldUseBatchSchemaForResolvingProjectedColumn && this.fieldInfoMap != null ?
      getComplexNameSegments(schemaPath) : schemaPath.getComplexNameSegments();
  }

  private SchemaPath getParquetColumnPath(SchemaPath pathInBatchSchema) {
    List<String> pathSegmentsInBatchSchema = shouldUseBatchSchemaForResolvingProjectedColumn && this.fieldInfoMap != null ?
      getComplexNameSegments(pathInBatchSchema) : pathInBatchSchema.getComplexNameSegments();
    List<String> pathSegmentsInParquet = getParquetSchemaColumnName(pathSegmentsInBatchSchema);
    return pathSegmentsInParquet == null ? null :
      SchemaPath.getCompoundPath(pathSegmentsInParquet.toArray(new String[0]));
  }

  /**
   * This Function takes schemaPath and by using batch schema, convert it to iceberg column name List of string( i.e, by concatenating the list of string using '.',
   *  we can get complete column name)
   *
   * For example: If table schema is like A Row(B Array(Row(id int, name varchar), c int)
   *  if projected Column is  A.B[1], we get schemaPath with following hierarchy
   *  NameSegment(A)
   *    NameSegment(B)
   *      ArraySegment[1]
   *   Using above schema path we want to convert to  => A.B.list.element
   *
   *  Similarly,
   *  (projected column  => expected column name)
   *   A.B.id => A.B.list.element.id
   *   A.B[0].id => A.B.list.element.id
   *   A.B => A.B
   */
  public List<String> getComplexNameSegments(SchemaPath schemaPath) {
    List<String> segments = new ArrayList<>();
    PathSegment seg = schemaPath.getRootSegment();
    boolean isListChild = false;
    Map<String, FieldInfo> currentChildMap = fieldInfoMap;

    while (seg != null) {
      Preconditions.checkNotNull(currentChildMap, "currentChildMap");
      if (seg.isArray() || isListChild) {
        segments.add("list");
        segments.add("element");
        FieldInfo fieldInfo = currentChildMap.getOrDefault("$data$", null);
        currentChildMap = fieldInfo != null ? fieldInfo.getChildFieldInfo() : null;
        isListChild = fieldInfo != null && fieldInfo.getType() != null && ArrowType.ArrowTypeID.List.equals(fieldInfo.getType());
        if (!seg.isArray()) {
          continue;
        }
      } else {
        String segmentName = seg.getNameSegment().getPath();
        segments.add(segmentName);
          // planner doesn't always send index with list path segment
        FieldInfo fieldInfo = currentChildMap.getOrDefault(segmentName.toLowerCase(), null);
        isListChild = fieldInfo != null && fieldInfo.getType() != null && "list".equalsIgnoreCase(fieldInfo.getType().toString());
        currentChildMap = fieldInfo != null ? fieldInfo.getChildFieldInfo() : null;
      }
      seg = seg.getChild();
    }
    return segments;
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
    if (columnInBatchSchema.size() == 1) {
      return Lists.newArrayList(columnInParquet);
    }
    return Lists.newArrayList(columnInParquet.split("\\."));
  }

  public List<String> convertColumnDescriptor(MessageType schema, ColumnDescriptor columnDescriptor) {
    return Lists.newArrayList(columnDescriptor.getPath());
  }

  public String toDotString(SchemaPath schemaPath, ValueVector vector) {
    return schemaPath.toDotString().toLowerCase();
  }

  private Map<String, FieldInfo> getFieldsMapForBatchSchema(BatchSchema batchSchema) {
    if (batchSchema == null || batchSchema.getFields() == null) {
      return null;
    }
    Map<String, FieldInfo> fieldsInfoMap = new HashMap<>();

    for (Field field : batchSchema.getFields()) {
      FieldInfo fieldInfo = new FieldInfo(field);
      if (fieldInfo.getName() != null) {
        fieldsInfoMap.put(fieldInfo.getName().toString(), fieldInfo);
      }
    }
    return fieldsInfoMap;
  }

  /**
   *  This class is for holding the children information of the children Fields in map format
   */
  class FieldInfo {
    private String name;
    private ArrowType.ArrowTypeID type;
    private Map<String, FieldInfo> childFieldInfo;

    public String getName() {
      return name;
    }

    public ArrowType.ArrowTypeID getType() {
      return type;
    }

    public Map<String, FieldInfo> getChildFieldInfo() {
      return childFieldInfo;
    }

    private FieldInfo(Field field) {
      this.childFieldInfo = new HashMap<>();
      this.name = null;
      this.type = null;
      if(field == null) {
        return;
      }

      if(field.getName() != null) {
        this.name = field.getName().toLowerCase();
      }

      if(field.getFieldType() != null && field.getFieldType().getType() != null) {
        this.type = field.getFieldType().getType().getTypeID();
      }

      if (field.getChildren() != null) {
        for (Field child : field.getChildren()) {
          FieldInfo fieldInfo = new FieldInfo(child);
          if(fieldInfo.getName() != null) {
            childFieldInfo.put(fieldInfo.getName().toLowerCase(), fieldInfo);
          }
        }
      }
    }
  }
}
