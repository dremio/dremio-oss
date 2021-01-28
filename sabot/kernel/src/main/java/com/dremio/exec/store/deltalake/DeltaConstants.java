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

/**
 * DeltaLake specific keywords and conventions
 */
public final class DeltaConstants {

    private DeltaConstants() {
        // Not to be instantiated
    }


    public static final String DELTA_LOG_DIR = "_delta_log";

    public static final String DELTA_FIELD_COMMIT_INFO = "commitInfo";
    public static final String DELTA_FIELD_PROTOCOL = "protocol";
    public static final String DELTA_FIELD_METADATA = "metaData";
    public static final String DELTA_FIELD_ADD = "add";
    public static final String DELTA_FIELD_REMOVE = "remove";

    public static final String DELTA_FIELD_METADATA_SCHEMA_STRING = "schemaString";
    public static final String DELTA_FIELD_METADATA_PARTITION_COLS = "partitionColumns";

    public static final String SCHEMA_STRING_FIELDS = "fields";
    public static final String SCHEMA_STRING_FIELDS_NAME = "name";
    public static final String SCHEMA_STRING_FIELDS_TYPE = "type";
    public static final String SCHEMA_STRING_FIELDS_NULLABLE = "nullable";
    public static final String SCHEMA_STRING_FIELDS_ARR_ELEMENT_TYPE = "elementType";
    public static final String SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL = "containsNull";

    public static final String DELTA_STRUCT = "struct";
    public static final String DELTA_ARRAY = "array";
    public static final String DELTA_BYTE = "byte";
    public static final String DELTA_SHORT = "short";
    public static final String DELTA_INT = "integer";
    public static final String DELTA_LONG = "long";
    public static final String DELTA_FLOAT = "float";
    public static final String DELTA_DOUBLE = "double";
    public static final String DELTA_BOOL = "boolean";
    public static final String DELTA_STRING = "string";
    public static final String DELTA_BINARY = "binary";
    public static final String DELTA_TIMESTAMP = "timestamp";
    public static final String DELTA_DATE = "date";
    public static final String DELTA_DECIMAL = "decimal";

    public static final String OP = "operation";
    public static final String OP_METRICS = "operationMetrics";
    public static final String OP_NUM_FILES = "numFiles";
    public static final String OP_NUM_ADDED_FILES = "numAddedFiles";
    public static final String OP_NUM_REMOVED_FILES = "numRemovedFiles";
    public static final String OP_NUM_OUTPUT_ROWS = "numOutputRows";
    public static final String OP_NUM_OUTPUT_BYTES = "numOutputBytes";
    public static final String OP_NUM_ADDED_BYTES = "numAddedBytes";
    public static final String OP_NUM_REMOVED_BYTES = "numRemovedBytes";
    public static final String DELTA_READ_VERSION = "readVersion";

    public static final String PROTOCOL_MIN_READER_VERSION = "minReaderVersion";
}
