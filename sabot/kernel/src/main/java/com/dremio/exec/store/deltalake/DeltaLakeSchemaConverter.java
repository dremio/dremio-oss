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

package com.dremio.exec.store.deltalake;

import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_ARRAY;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_BINARY;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_BOOL;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_BYTE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_DATE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_DOUBLE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_ENTRIES;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FLOAT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_INT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_LONG;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_MAP;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_SHORT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_STRING;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_STRUCT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_KEY;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_ARR_ELEMENT_TYPE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_METADATA;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_NAME;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_NULLABLE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_TYPE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_MAP_KEY_TYPE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_MAP_VALUE_CONTAINS_NULL;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_MAP_VALUE_TYPE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_VALUE;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Conversion utility for translating DeltaLake schema to arrow */
public final class DeltaLakeSchemaConverter {
  private static final Logger logger = LoggerFactory.getLogger(DeltaLakeSchemaConverter.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final boolean isMapDataTypeEnabled;
  private final DeltaColumnMappingMode columnMappingMode;

  private DeltaLakeSchemaConverter(
      boolean isMapDataTypeEnabled, DeltaColumnMappingMode columnMappingMode) {
    this.isMapDataTypeEnabled = isMapDataTypeEnabled;
    this.columnMappingMode = columnMappingMode;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private boolean isMapDataTypeEnabled = false;
    private DeltaColumnMappingMode columnMappingMode = DeltaColumnMappingMode.NONE;

    public Builder withMapEnabled(boolean isMapDataTypeEnabled) {
      this.isMapDataTypeEnabled = isMapDataTypeEnabled;
      return this;
    }

    public Builder withColumnMapping(DeltaColumnMappingMode columnMappingMode) {
      this.columnMappingMode = columnMappingMode;
      return this;
    }

    public DeltaLakeSchemaConverter build() {
      return new DeltaLakeSchemaConverter(isMapDataTypeEnabled, columnMappingMode);
    }
  }

  public BatchSchema fromSchemaString(String schemaString) throws IOException {
    Preconditions.checkNotNull(schemaString);
    final JsonNode schemaJson = OBJECT_MAPPER.readTree(schemaString);
    final JsonNode fieldsJson = schemaJson.get(SCHEMA_STRING_FIELDS);
    Preconditions.checkNotNull(
        fieldsJson, "Schema string doesn't contain any fields: %s", schemaString);
    return new BatchSchema(
        StreamSupport.stream(fieldsJson.spliterator(), false)
            .map(this::fromFieldJson)
            .filter(Objects::nonNull)
            .collect(Collectors.toList()));
  }

  private Field fromFieldJson(final JsonNode fields) {
    final JsonNode typeNode = fields.get(SCHEMA_STRING_FIELDS_TYPE);
    final String name = fields.get(SCHEMA_STRING_FIELDS_NAME).asText();
    final boolean isNullable = fields.get(SCHEMA_STRING_FIELDS_NULLABLE).asBoolean(true);
    final Map<String, String> metadata = readMetadataNode(fields);
    return fromType(name, typeNode, isNullable, metadata);
  }

  private Map<String, String> readMetadataNode(final JsonNode fields) {
    if (columnMappingMode == DeltaColumnMappingMode.NONE
        || !fields.has(SCHEMA_STRING_FIELDS_METADATA)) {
      return null;
    }

    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                fields.get(SCHEMA_STRING_FIELDS_METADATA).fields(), 0),
            false)
        .filter(
            entry -> {
              switch (columnMappingMode) {
                case NAME:
                  return entry.getKey().equals(SCHEMA_STRING_FIELDS_COLUMN_MAPPING_PHYSICAL_NAME);
                case ID:
                  // fallthrough to preserve physical name for partitions and stats mapping
                default:
                  // fallback to return all metadata entries
                  return true;
              }
            })
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().asText()));
  }

  private Field fromType(
      final String name,
      final JsonNode typeNode,
      final boolean isNullable,
      final Map<String, String> metadata) {
    // Complex fields result into an ObjectNode
    if (!typeNode.isObject()) {
      final FieldType fieldType = fromPrimitiveType(typeNode.asText(), isNullable);
      return new Field(
          name,
          new FieldType(isNullable, fieldType.getType(), fieldType.getDictionary(), metadata),
          Lists.newArrayList());
    } else if (DELTA_STRUCT.equalsIgnoreCase(typeNode.get(SCHEMA_STRING_FIELDS_TYPE).asText(""))) {
      final JsonNode structField = typeNode.get(SCHEMA_STRING_FIELDS);
      final List<Field> children =
          StreamSupport.stream(structField.spliterator(), false)
              .map(this::fromFieldJson)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      return children.isEmpty()
          ? null
          : new Field(
              name, new FieldType(isNullable, new ArrowType.Struct(), null, metadata), children);
    } else if (DELTA_ARRAY.equalsIgnoreCase(typeNode.get(SCHEMA_STRING_FIELDS_TYPE).asText(""))) {
      final FieldType listType = new FieldType(isNullable, new ArrowType.List(), null, metadata);
      final JsonNode elemType = typeNode.get(SCHEMA_STRING_FIELDS_ARR_ELEMENT_TYPE);
      final boolean containsNull =
          typeNode.get(SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL).asBoolean(false);
      final Field nestedField = fromType(ListVector.DATA_VECTOR_NAME, elemType, containsNull, null);
      return nestedField == null
          ? null
          : new Field(name, listType, Collections.singletonList(nestedField));
    } else if (DELTA_MAP.equalsIgnoreCase(typeNode.get(SCHEMA_STRING_FIELDS_TYPE).asText(""))
        && isMapDataTypeEnabled) {
      final JsonNode keyType = typeNode.get(SCHEMA_STRING_MAP_KEY_TYPE);
      final JsonNode valueType = typeNode.get(SCHEMA_STRING_MAP_VALUE_TYPE);
      if (keyType.isObject()) {
        return null;
      }
      final FieldType keyFieldType = fromPrimitiveType(keyType.asText(), false);
      final FieldType mapType = new FieldType(isNullable, new ArrowType.Map(false), null, metadata);
      final boolean valueContainsNull =
          typeNode.get(SCHEMA_STRING_MAP_VALUE_CONTAINS_NULL).asBoolean(false);
      final Field valueField = fromType(SCHEMA_VALUE, valueType, valueContainsNull, null);
      if (valueField == null) {
        return null;
      }
      final List<Field> structChildren = Lists.newArrayList();
      structChildren.add(new Field(SCHEMA_KEY, keyFieldType, null));
      structChildren.add(valueField);
      final Field structMap =
          new Field(
              DELTA_ENTRIES, new FieldType(false, new ArrowType.Struct(), null), structChildren);
      return new Field(name, mapType, Collections.singletonList(structMap));
    } else {
      // drop all other unknown DeltaLake complex column types
      return null;
    }
  }

  public static FieldType fromPrimitiveType(final String type, final boolean isNullable) {

    if (type.startsWith("decimal")) {
      // extract precision and scale
      String[] scaleAndPrecision = type.split("\\s*[()]\\s*")[1].split(",");
      ArrowType decimal =
          new ArrowType.Decimal(
              Integer.parseInt(scaleAndPrecision[0]), Integer.parseInt(scaleAndPrecision[1]), 128);
      return new FieldType(isNullable, decimal, null);
    }

    switch (type) {
      case DELTA_BYTE:
      case DELTA_SHORT:
      case DELTA_INT:
        return new FieldType(isNullable, new ArrowType.Int(32, true), null);
      case DELTA_LONG:
        return new FieldType(isNullable, new ArrowType.Int(64, true), null);
      case DELTA_FLOAT:
        return new FieldType(
            isNullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
      case DELTA_DOUBLE:
        return new FieldType(
            isNullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
      case DELTA_BOOL:
        return new FieldType(isNullable, new ArrowType.Bool(), null);
      case DELTA_STRING:
        return new FieldType(isNullable, new ArrowType.Utf8(), null);
      case DELTA_BINARY:
        return new FieldType(isNullable, new ArrowType.Binary(), null);
      case DELTA_TIMESTAMP:
        return new FieldType(isNullable, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null), null);
      case DELTA_DATE:
        return new FieldType(isNullable, new ArrowType.Date(DateUnit.MILLISECOND), null);
      default:
        logger.error("Unsupported type : " + type);
        throw new UnsupportedOperationException("Unsupported type : " + type);
    }
  }
}
