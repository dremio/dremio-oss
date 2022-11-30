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

import java.util.Arrays;
import java.util.List;

/**
 * DeltaLake specific keywords and conventions
 */
public final class DeltaConstants {

    private DeltaConstants() {
        // Not to be instantiated
    }

    public static final String DELTA_FIELD_JOINER = "_";
    public static final String DELTA_LOG_DIR = "_delta_log";
    public static final String DELTA_LAST_CHECKPOINT = "_last_checkpoint";

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
    public static final String SCHEMA_STRING_MAP_KEY_TYPE = "keyType";
    public static final String SCHEMA_STRING_MAP_VALUE_TYPE = "valueType";
    public static final String SCHEMA_STRING_MAP_VALUE_CONTAINS_NULL = "valueContainsNull";
    public static final String SCHEMA_STRING_FIELDS_ARR_CONTAINS_NULL = "containsNull";

    public static final String DELTA_FIELD_ADD_SIZE = "size";
    public static final String DELTA_FIELD_ADD_STATS_PARSED = "stats_parsed";
    public static final String DELTA_FIELD_ADD_PATH = "path";

    public static final String STATS_PARSED_NUM_RECORDS = "numRecords";

    public static final String DELTA_STRUCT = "struct";
    public static final String DELTA_ENTRIES = "entries";
    public static final String DELTA_ARRAY = "array";
    public static final String DELTA_MAP = "map";
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
    public static final String OP_NUM_DELETED_ROWS = "numDeletedRows";
    public static final String OP_NUM_TARGET_FILES_ADDED = "numTargetFilesAdded";
    public static final String OP_NUM_TARGET_FILES_REMOVED = "numTargetFilesRemoved";
    public static final String OP_NUM_TARGET_ROWS_INSERTED = "numTargetRowsInserted";
    public static final String OP_NUM_TARGET_ROWS_DELETED = "numTargetRowsDeleted";

    public static final String OPERATION_UNKNOWN = "UNKNOWN";
    public static final String OPERATION_COMBINED = "COMBINED";
    public static final String OPERATION_TRUNCATE = "TRUNCATE";
    public static final String OPERATION_MERGE = "MERGE";
    public static final String OPERATION_DELETE = "DELETE";
    public static final String OPERATION_WRITE = "WRITE";
    public static final String OPERATION_ADD_COLUMNS = "ADD COLUMNS";
    public static final String OPERATION_OPTIMIZE = "OPTIMIZE";
    public static final String OPERATION_CREATE_TABLE = "CREATE TABLE";
    public static final String OPERATION_CREATE_TABLE_AS_SELECT = "CREATE TABLE AS SELECT";
    public static final String OPERATION_CREATE_OR_REPLACE_TABLE = "CREATE OR REPLACE TABLE";
    public static final String OPERATION_CREATE_OR_REPLACE_TABLE_AS_SELECT = "CREATE OR REPLACE TABLE AS SELECT";
    public static final List<String> CREATE_OPERATIONS = Arrays.asList(OPERATION_CREATE_TABLE, OPERATION_CREATE_TABLE_AS_SELECT, OPERATION_CREATE_OR_REPLACE_TABLE, OPERATION_CREATE_OR_REPLACE_TABLE_AS_SELECT);

    public static final String PROTOCOL_MIN_READER_VERSION = "minReaderVersion";

    public static final String SCHEMA_PATH = "path";
    public static final String SCHEMA_KEY = "key";
    public static final String SCHEMA_VALUE = "value";
    public static final String SCHEMA_KEY_VALUE = "key_value";
    public static final String SCHEMA_PARTITION_VALUES = "partitionValues";
    public static final String SCHEMA_SIZE = "size";
    public static final String SCHEMA_MODIFICATION_TIME = "modificationTime";
    public static final String SCHEMA_DATA_CHANGE = "dataChange";
    public static final String SCHEMA_TAGS = "tags";
    public static final String SCHEMA_STATS = "stats";
    public static final String SCHEMA_PARTITION_VALUES_PARSED = "partitionValues_parsed";
    public static final String SCHEMA_MIN_VALUES = "minValues";
    public static final String SCHEMA_MAX_VALUES = "maxValues";
    public static final String SCHEMA_NULL_COUNT = "nullCount";
    public static final String SCHEMA_NUM_RECORDS = "numRecords";
    public static final String SCHEMA_STATS_PARSED = "stats_parsed";
    public static final String SCHEMA_DELETION_TIMESTAMP = "deletionTimestamp";
    public static final String VERSION = "version";
    public static final String SCHEMA_ADD_PATH = DELTA_FIELD_ADD + DELTA_FIELD_JOINER + SCHEMA_PATH;
    public static final String SCHEMA_REMOVE_PATH = DELTA_FIELD_REMOVE + DELTA_FIELD_JOINER + SCHEMA_PATH;
    public static final String SCHEMA_ADD_VERSION = DELTA_FIELD_ADD + DELTA_FIELD_JOINER + VERSION;
    public static final String SCHEMA_REMOVE_VERSION = DELTA_FIELD_REMOVE + DELTA_FIELD_JOINER + VERSION;
    public static final String SCHMEA_ADD_DATACHANGE = DELTA_FIELD_ADD + DELTA_FIELD_JOINER + SCHEMA_DATA_CHANGE;
    public static final String SCHEMA_ADD_SIZE = DELTA_FIELD_ADD + DELTA_FIELD_JOINER + SCHEMA_SIZE;
    public static final String SCHEMA_ADD_MODIFICATION_TIME = DELTA_FIELD_ADD + DELTA_FIELD_JOINER + SCHEMA_MODIFICATION_TIME;

    // Suffix for partition column names as we want to avoid any clashes with non-partition column names.
    public static final String PARTITION_NAME_SUFFIX = "_val";
}
