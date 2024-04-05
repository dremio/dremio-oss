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

package com.dremio.exec.store.ischema.metadata;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.record.BatchSchema;
import com.google.common.collect.ImmutableList;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.arrow.vector.types.pojo.Field;

/** Captures metadata for tables in "INFORMATION_SCHEMA". */
public final class InformationSchemaMetadata {

  // CATALOGS table
  public static final Field CATALOG_NAME = CompleteType.VARCHAR.toField("CATALOG_NAME");
  public static final Field CATALOG_DESCRIPTION =
      CompleteType.VARCHAR.toField("CATALOG_DESCRIPTION");
  public static final Field CATALOG_CONNECT = CompleteType.VARCHAR.toField("CATALOG_CONNECT");

  // SCHEMATA table
  // public static final Field CATALOG_NAME = CompleteType.VARCHAR.toField("CATALOG_NAME");
  public static final Field SCHEMA_NAME = CompleteType.VARCHAR.toField("SCHEMA_NAME");
  public static final Field SCHEMA_OWNER = CompleteType.VARCHAR.toField("SCHEMA_OWNER");
  public static final Field TYPE = CompleteType.VARCHAR.toField("TYPE");
  public static final Field IS_MUTABLE = CompleteType.VARCHAR.toField("IS_MUTABLE");

  // TABLES table
  public static final Field TABLE_CATALOG = CompleteType.VARCHAR.toField("TABLE_CATALOG");
  public static final Field TABLE_SCHEMA = CompleteType.VARCHAR.toField("TABLE_SCHEMA");
  public static final Field TABLE_NAME = CompleteType.VARCHAR.toField("TABLE_NAME");
  public static final Field TABLE_TYPE = CompleteType.VARCHAR.toField("TABLE_TYPE");

  // VIEWS table
  // public static final Field TABLE_CATALOG = CompleteType.VARCHAR.toField("TABLE_CATALOG");
  // public static final Field TABLE_SCHEMA = CompleteType.VARCHAR.toField("TABLE_SCHEMA");
  // public static final Field TABLE_NAME = CompleteType.VARCHAR.toField("TABLE_NAME");
  public static final Field VIEW_DEFINITION = CompleteType.VARCHAR.toField("VIEW_DEFINITION");

  // COLUMNS table
  // public static final Field TABLE_CATALOG = CompleteType.VARCHAR.toField("TABLE_CATALOG");
  // public static final Field TABLE_SCHEMA = CompleteType.VARCHAR.toField("TABLE_SCHEMA");
  // public static final Field TABLE_NAME = CompleteType.VARCHAR.toField("TABLE_NAME");
  public static final Field COLUMN_NAME = CompleteType.VARCHAR.toField("COLUMN_NAME");
  public static final Field ORDINAL_POSITION = CompleteType.INT.toField("ORDINAL_POSITION");
  public static final Field COLUMN_DEFAULT = CompleteType.VARCHAR.toField("COLUMN_DEFAULT");
  public static final Field IS_NULLABLE = CompleteType.VARCHAR.toField("IS_NULLABLE");
  public static final Field DATA_TYPE = CompleteType.VARCHAR.toField("DATA_TYPE");
  public static final Field COLUMN_SIZE = CompleteType.INT.toField("COLUMN_SIZE");
  public static final Field CHARACTER_MAXIMUM_LENGTH =
      CompleteType.INT.toField("CHARACTER_MAXIMUM_LENGTH");
  public static final Field CHARACTER_OCTET_LENGTH =
      CompleteType.INT.toField("CHARACTER_OCTET_LENGTH");
  public static final Field NUMERIC_PRECISION = CompleteType.INT.toField("NUMERIC_PRECISION");
  public static final Field NUMERIC_PRECISION_RADIX =
      CompleteType.INT.toField("NUMERIC_PRECISION_RADIX");
  public static final Field NUMERIC_SCALE = CompleteType.INT.toField("NUMERIC_SCALE");
  public static final Field DATETIME_PRECISION = CompleteType.INT.toField("DATETIME_PRECISION");
  public static final Field INTERVAL_TYPE = CompleteType.VARCHAR.toField("INTERVAL_TYPE");
  public static final Field INTERVAL_PRECISION = CompleteType.INT.toField("INTERVAL_PRECISION");

  private static final BatchSchema CATALOGS_SCHEMA;
  private static final BatchSchema SCHEMATA_SCHEMA;
  private static final BatchSchema TABLES_SCHEMA;
  private static final BatchSchema VIEWS_SCHEMA;
  private static final BatchSchema COLUMNS_SCHEMA;

  static {
    CATALOGS_SCHEMA =
        new BatchSchema(ImmutableList.of(CATALOG_NAME, CATALOG_DESCRIPTION, CATALOG_CONNECT));
    SCHEMATA_SCHEMA =
        new BatchSchema(
            ImmutableList.of(CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER, TYPE, IS_MUTABLE));
    TABLES_SCHEMA =
        new BatchSchema(ImmutableList.of(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE));
    VIEWS_SCHEMA =
        new BatchSchema(ImmutableList.of(TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, VIEW_DEFINITION));
    COLUMNS_SCHEMA =
        new BatchSchema(
            ImmutableList.of(
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                ORDINAL_POSITION,
                COLUMN_DEFAULT,
                IS_NULLABLE,
                DATA_TYPE,
                COLUMN_SIZE,
                CHARACTER_MAXIMUM_LENGTH,
                CHARACTER_OCTET_LENGTH,
                NUMERIC_PRECISION,
                NUMERIC_PRECISION_RADIX,
                NUMERIC_SCALE,
                DATETIME_PRECISION,
                INTERVAL_TYPE,
                INTERVAL_PRECISION));
  }

  public static BatchSchema getCatalogsSchema() {
    return CATALOGS_SCHEMA;
  }

  public static BatchSchema getSchemataSchema() {
    return SCHEMATA_SCHEMA;
  }

  public static BatchSchema getTablesSchema() {
    return TABLES_SCHEMA;
  }

  public static BatchSchema getViewsSchema() {
    return VIEWS_SCHEMA;
  }

  public static BatchSchema getColumnsSchema() {
    return COLUMNS_SCHEMA;
  }

  public static Set<String> getAllFieldNames(BatchSchema recordSchema) {
    return recordSchema.getFields().stream().map(Field::getName).collect(Collectors.toSet());
  }

  private InformationSchemaMetadata() {}
}
