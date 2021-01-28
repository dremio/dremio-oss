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
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_DECIMAL;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_DOUBLE;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_FLOAT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_INT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_LONG;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_SHORT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_STRING;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_STRUCT;
import static com.dremio.exec.store.deltalake.DeltaConstants.DELTA_TIMESTAMP;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_ARR_ELEMENT_TYPE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_NAME;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_NULLABLE;
import static com.dremio.exec.store.deltalake.DeltaConstants.SCHEMA_STRING_FIELDS_TYPE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Conversion utility for translating DeltaLake schema to arrow
 */
public class DeltaLakeSchemaConverter {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLakeSchemaConverter.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    /**
     * NOTE: This function impl is done in order to facilitate E2E validations and is not the final version.
     * Final version will be covered as part of DX-26748
     */
    public static BatchSchema fromSchemaString(String schemaString) throws IOException {
        Preconditions.checkNotNull(schemaString);
        final JsonNode schemaJson = OBJECT_MAPPER.readTree(schemaString);
        final JsonNode fieldsJson = schemaJson.get(SCHEMA_STRING_FIELDS);
        Preconditions.checkNotNull(fieldsJson, "Schema string doesn't contain any fields - {}", schemaString);

        return new BatchSchema(StreamSupport.stream(fieldsJson.spliterator(), false)
                .map(DeltaLakeSchemaConverter::fromFieldJson)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    private static Field fromFieldJson(final JsonNode fields) {
        final JsonNode typeNode = fields.get(SCHEMA_STRING_FIELDS_TYPE);
        final String name = fields.get(SCHEMA_STRING_FIELDS_NAME).asText();
        final boolean isNullable = fields.get(SCHEMA_STRING_FIELDS_NULLABLE).asBoolean(true);
        return fromType(name, typeNode, isNullable);
    }

    private static Field fromType(final String name, final JsonNode typeNode, final boolean isNullable) {
        // Complex fields result into an ObjectNode
        if (!typeNode.isObject()) {
            final FieldType fieldType = fromPrimitiveType(typeNode.asText(), isNullable);
            return fieldType == null ? null : new Field(name, isNullable, fieldType.getType(), new ArrayList<>());
        } else if (DELTA_STRUCT.equalsIgnoreCase(typeNode.get(SCHEMA_STRING_FIELDS_TYPE).asText(""))) {
            final JsonNode structField = typeNode.get(SCHEMA_STRING_FIELDS);
            final List<Field> children = StreamSupport.stream(structField.spliterator(), false)
                    .map(DeltaLakeSchemaConverter::fromFieldJson)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            return children.isEmpty() ? null : new Field(name, new FieldType(isNullable, new ArrowType.Struct(), null), children);
        } else if (DELTA_ARRAY.equalsIgnoreCase(typeNode.get(SCHEMA_STRING_FIELDS_TYPE).asText(""))) {
            final FieldType listType = new FieldType(isNullable, new ArrowType.List(), null);
            final JsonNode elemType = typeNode.get(SCHEMA_STRING_FIELDS_ARR_ELEMENT_TYPE);
            final boolean containsNull = typeNode.get(SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL).asBoolean(false);
            final Field nestedField = fromType(ListVector.DATA_VECTOR_NAME, elemType, containsNull);
            return nestedField == null ? null : new Field(name, listType, Collections.singletonList(nestedField));
        } else {
            // drop map type and all other unknown DeltaLake complex column types
            return null;
        }
    }

    public static FieldType fromPrimitiveType(final String type, final boolean isNullable) {
        switch (type) {
            case DELTA_BYTE:
                return new FieldType(isNullable, new ArrowType.Int(8, true), null);
            case DELTA_SHORT:
                return new FieldType(isNullable, new ArrowType.Int(16, true), null);
            case DELTA_INT:
                return new FieldType(isNullable, new ArrowType.Int(32, true), null);
            case DELTA_LONG:
                return new FieldType(isNullable, new ArrowType.Int(64, true), null);
            case DELTA_FLOAT:
                return new FieldType(isNullable, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
            case DELTA_DOUBLE:
                return new FieldType(isNullable, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
            case DELTA_BOOL:
                return new FieldType(isNullable, new ArrowType.Bool(), null);
            case DELTA_STRING:
                return new FieldType(isNullable, new ArrowType.Utf8(), null);
            case DELTA_BINARY:
                return new FieldType(isNullable, new ArrowType.Binary(), null);
            // TODO: Map remaining Delta Lake types
            case DELTA_TIMESTAMP:
            case DELTA_DATE:
            case DELTA_DECIMAL:
                return null;
            default:
                logger.error("Unsupported type : " + type);
                throw new UnsupportedOperationException("Unsupported type : " + type);
        }
    }
}
