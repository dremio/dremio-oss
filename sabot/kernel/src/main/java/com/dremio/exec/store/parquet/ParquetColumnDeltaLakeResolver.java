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

import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_COLUMN_MAPPING_ID;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME;

import com.dremio.common.expression.SchemaPath;
import com.dremio.common.map.CaseInsensitiveMap;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * This class maps column names from batch schema to parquet schema and vice versa for DeltaLake
 * datasets. If field metadata contains "id", then parquetSchema is used to get column names in
 * parquet file, otherwise field metadata "physicalName" is used to create mappings.
 */
public class ParquetColumnDeltaLakeResolver implements ParquetColumnResolver {
  private final List<SchemaPath> projectedColumns;
  private final CaseInsensitiveMap<String> schemaToParquet;
  private final CaseInsensitiveMap<String> parquetToSchema;

  public ParquetColumnDeltaLakeResolver(
      List<SchemaPath> projectedColumns, BatchSchema batchSchema, MessageType parquetSchema) {
    this.projectedColumns = projectedColumns;
    this.schemaToParquet = CaseInsensitiveMap.newHashMap();
    this.parquetToSchema = CaseInsensitiveMap.newHashMap();
    if (hasMetadataProperty(batchSchema, SCHEMA_STRING_FIELDS_COLUMN_MAPPING_ID)
        && parquetSchema != null) {
      Map<Integer, String> columnIdToParquetName = Maps.newHashMap();
      createColumnIdToParquetNameMap(parquetSchema.getFields(), columnIdToParquetName);
      initializeMapping(
          batchSchema.getFields(), field -> resolveFieldNameById(field, columnIdToParquetName));
    } else if (hasMetadataProperty(
        batchSchema, SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME)) {
      initializeMapping(
          batchSchema.getFields(), ParquetColumnDeltaLakeResolver::resolveFieldNameByPhysicalName);
    }
  }

  public static boolean hasColumnMapping(BatchSchema schema) {
    return hasMetadataProperty(schema, SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME)
        || hasMetadataProperty(schema, SCHEMA_STRING_FIELDS_COLUMN_MAPPING_ID);
  }

  private static boolean hasMetadataProperty(BatchSchema schema, String propertyName) {
    if (schema == null) {
      return false;
    }
    int mappingCount = 0;
    for (Field field : schema.getFields()) {
      if (field.getMetadata() != null && field.getMetadata().containsKey(propertyName)) {
        mappingCount++;
      }
    }
    if (mappingCount == 0) {
      return false;
    }
    return mappingCount == schema.getFieldCount();
  }

  @Override
  public List<SchemaPath> getBatchSchemaProjectedColumns() {
    return projectedColumns;
  }

  @Override
  public List<SchemaPath> getProjectedParquetColumns() {
    return getBatchSchemaProjectedColumns().stream()
        .map(this::getParquetColumnPath)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public String getBatchSchemaColumnName(String columnInParquetFile) {
    return parquetToSchema.getOrDefault(columnInParquetFile, columnInParquetFile);
  }

  @Override
  public List<String> getBatchSchemaColumnName(List<String> columnInParquetFile) {
    String parquetName = String.join(".", columnInParquetFile);
    String columnInSchema = getBatchSchemaColumnName(parquetName);
    if (columnInSchema == null) {
      return null;
    }
    if (columnInParquetFile.size() == 1) {
      return Lists.newArrayList(columnInSchema);
    }
    return Lists.newArrayList(columnInSchema.split("\\."));
  }

  @Override
  public String getParquetColumnName(String columnInBatchSchema) {
    return schemaToParquet.getOrDefault(columnInBatchSchema, columnInBatchSchema);
  }

  private List<String> getParquetColumnName(List<String> columnInBatchSchema) {
    String columnName = String.join(".", columnInBatchSchema);
    String columnInParquet = getParquetColumnName(columnName);
    if (columnInParquet == null) {
      return null;
    }
    if (columnInBatchSchema.size() == 1) {
      return Lists.newArrayList(columnInParquet);
    }
    return Lists.newArrayList(columnInParquet.split("\\."));
  }

  @Override
  public List<SchemaPath> getBatchSchemaColumns(List<SchemaPath> parquetSchemaPaths) {
    return parquetSchemaPaths.stream()
        .map(this::getBatchSchemaColumnPath)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  @Override
  public SchemaPath getBatchSchemaColumnPath(SchemaPath pathInParquetFile) {
    List<String> pathSegmentsInParquet = pathInParquetFile.getComplexNameSegments();
    List<String> pathSegmentsInSchema = getBatchSchemaColumnName(pathSegmentsInParquet);
    return pathSegmentsInSchema == null
        ? null
        : SchemaPath.getCompoundPath(pathSegmentsInSchema.toArray(new String[0]));
  }

  private SchemaPath getParquetColumnPath(SchemaPath pathInBatchSchema) {
    List<String> pathSegmentsInBatchSchema = pathInBatchSchema.getComplexNameSegments();
    List<String> pathSegmentsInParquet = getParquetColumnName(pathSegmentsInBatchSchema);
    return pathSegmentsInParquet == null
        ? null
        : SchemaPath.getCompoundPath(pathSegmentsInParquet.toArray(new String[0]));
  }

  @Override
  public List<String> getNameSegments(SchemaPath schemaPath) {
    return schemaPath.getComplexNameSegments();
  }

  @Override
  public List<String> convertColumnDescriptor(
      MessageType schema, ColumnDescriptor columnDescriptor) {
    return Lists.newArrayList(columnDescriptor.getPath());
  }

  @Override
  public String toDotString(SchemaPath schemaPath, ValueVector vector) {
    return schemaPath.toDotString().toLowerCase();
  }

  private void createColumnIdToParquetNameMap(
      List<Type> fields, Map<Integer, String> columnIdToParquetName) {
    for (Type field : fields) {
      if (field.getId() != null) {
        columnIdToParquetName.put(field.getId().intValue(), field.getName());
      }
      if (!field.isPrimitive()) {
        createColumnIdToParquetNameMap(field.asGroupType().getFields(), columnIdToParquetName);
      }
    }
  }

  private static String resolveFieldNameById(
      Field field, Map<Integer, String> columnIdToParquetName) {
    String idString = field.getMetadata().get(SCHEMA_STRING_FIELDS_COLUMN_MAPPING_ID);
    if (idString == null) {
      return null;
    }
    return columnIdToParquetName.get(Integer.parseInt(idString));
  }

  private static String resolveFieldNameByPhysicalName(Field field) {
    return field.getMetadata().get(SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME);
  }

  private void initializeMapping(List<Field> fields, Function<Field, String> resolveName) {
    for (Field field : fields) {
      String fieldName = field.getName();
      String parquetName = resolveName.apply(field);
      this.schemaToParquet.put(fieldName, parquetName);
      this.parquetToSchema.put(parquetName, fieldName);
      addColumnMapping(
          new String[] {fieldName}, new String[] {parquetName}, field.getChildren(), resolveName);
    }
  }

  private static String[] appendString(String[] arr, String value) {
    final int size = arr.length;
    arr = Arrays.copyOf(arr, size + 1);
    arr[size] = value;
    return arr;
  }

  private void addColumnMapping(
      String[] baseSchemaPath,
      String[] baseParquetPath,
      List<Field> fields,
      Function<Field, String> resolveName) {
    for (Field field : fields) {
      String fieldName = field.getName();
      String parquetName = resolveName.apply(field);
      if (parquetName == null) {
        if ("$data$".equals(fieldName)) {
          fieldName = "list.element";
          parquetName = fieldName;
        } else if ("entries".equals(fieldName)) {
          // Dremio uses "entries" and Iceberg/Parquet uses "key_value"
          parquetName = "key_value";
        } else {
          parquetName = fieldName;
        }
      } else {
        this.parquetToSchema.put(parquetName, fieldName);
      }

      String[] parquetPath = appendString(baseParquetPath, parquetName);
      String[] schemaPath = appendString(baseSchemaPath, fieldName);

      this.parquetToSchema.put(String.join(".", parquetPath), String.join(".", schemaPath));
      addColumnMapping(schemaPath, parquetPath, field.getChildren(), resolveName);
    }
  }
}
