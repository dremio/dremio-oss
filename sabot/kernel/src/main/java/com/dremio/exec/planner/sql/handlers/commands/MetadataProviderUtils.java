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

package com.dremio.exec.planner.sql.handlers.commands;

import java.util.stream.Stream;

import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.sql.CalciteArrowHelper;
import com.dremio.exec.planner.types.JavaTypeFactoryImpl;
import com.dremio.exec.proto.UserProtos.CatalogMetadata;
import com.dremio.exec.proto.UserProtos.ColumnMetadata;
import com.dremio.exec.proto.UserProtos.SchemaMetadata;
import com.dremio.exec.proto.UserProtos.TableMetadata;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ischema.Column;
import com.dremio.service.catalog.Catalog;
import com.dremio.service.catalog.Schema;
import com.dremio.service.catalog.SchemaType;
import com.dremio.service.catalog.Table;
import com.dremio.service.catalog.TableSchema;
import com.dremio.service.catalog.TableType;
import com.google.common.base.Strings;

/**
 * Utilities functions related to {@link MetadataProvider}.
 */
final class MetadataProviderUtils {

  // prevent instantiation
  private MetadataProviderUtils() {
  }

  /**
   * Converts from {@link Catalog} to {@link CatalogMetadata}.
   *
   * @param catalog        catalog
   * @param defaultCatalog default catalog override
   * @return catalog metadata
   */
  static CatalogMetadata toCatalogMetadata(Catalog catalog, String defaultCatalog) {
    final CatalogMetadata.Builder builder = CatalogMetadata.newBuilder();
    builder.setCatalogName(Strings.isNullOrEmpty(defaultCatalog) ? catalog.getCatalogName() : defaultCatalog);
    builder.setConnect(catalog.getCatalogConnect());
    builder.setDescription(catalog.getCatalogDescription());
    return builder.build();
  }

  /**
   * Converts from {@link Schema} to {@link SchemaMetadata}.
   *
   * @param schema         schema
   * @param defaultCatalog default catalog override
   * @return schema metadata
   */
  static SchemaMetadata toSchemaMetadata(Schema schema, String defaultCatalog) {
    final SchemaMetadata.Builder builder = SchemaMetadata.newBuilder();
    builder.setCatalogName(Strings.isNullOrEmpty(defaultCatalog) ? schema.getCatalogName() : defaultCatalog);
    builder.setSchemaName(schema.getSchemaName());
    builder.setOwner(schema.getSchemaOwner());
    builder.setType(toSchemaType(schema.getSchemaType()));
    builder.setMutable(schema.getIsMutable() ? "YES" : "NO");
    return builder.build();
  }

  private static String toSchemaType(SchemaType schemaType) {
    switch (schemaType) {
    case SIMPLE:
      return "simple";
    case UNKNOWN_SCHEMA_TYPE:
    case UNRECOGNIZED:
    default:
      throw new UnsupportedOperationException("Unknown type: " + schemaType);
    }
  }

  /**
   * Converts from {@link Table} to {@link TableMetadata}.
   *
   * @param table          table
   * @param defaultCatalog default catalog override
   * @return table metadata
   */
  static TableMetadata toTableMetadata(Table table, String defaultCatalog) {
    final TableMetadata.Builder builder = TableMetadata.newBuilder();
    builder.setCatalogName(Strings.isNullOrEmpty(defaultCatalog) ? table.getCatalogName() : defaultCatalog);
    builder.setSchemaName(table.getSchemaName());
    builder.setTableName(table.getTableName());
    builder.setType(toTableType(table.getTableType()));
    return builder.build();
  }

  private static String toTableType(TableType tableType) {
    switch (tableType) {
    case TABLE:
      return "TABLE";
    case SYSTEM_TABLE:
      return "SYSTEM_TABLE";
    case VIEW:
      return "VIEW";
    case UNKNOWN_TABLE_TYPE:
    case UNRECOGNIZED:
    default:
      throw new UnsupportedOperationException("Unknown type: " + tableType);
    }
  }

  /**
   * Convert from {@link TableSchema} to {@link ColumnMetadata}.
   *
   * @param tableSchema    table schema
   * @param defaultCatalog default catalog override
   * @return column metadata
   */
  public static Stream<ColumnMetadata> toColumnMetadata(TableSchema tableSchema, String defaultCatalog, boolean complexTypeSupport) {
    final RelDataType rowType =
      CalciteArrowHelper.wrap(BatchSchema.deserialize(tableSchema.getBatchSchema().toByteArray()))
        .toCalciteRecordType(JavaTypeFactoryImpl.INSTANCE, complexTypeSupport);
    return rowType.getFieldList().stream()
      .map(field -> new Column(Strings.isNullOrEmpty(defaultCatalog) ? tableSchema.getCatalogName() : defaultCatalog,
        tableSchema.getSchemaName(),
        tableSchema.getTableName(),
        field
      ))
      .map(column -> {
        final ColumnMetadata.Builder builder = ColumnMetadata.newBuilder();
        builder.setCatalogName(column.TABLE_CATALOG);
        builder.setSchemaName(column.TABLE_SCHEMA);
        builder.setTableName(column.TABLE_NAME);
        builder.setColumnName(column.COLUMN_NAME);
        builder.setOrdinalPosition(column.ORDINAL_POSITION);
        if (column.COLUMN_DEFAULT != null) {
          builder.setDefaultValue(column.COLUMN_DEFAULT);
        }
        if ("YES".equalsIgnoreCase(column.IS_NULLABLE)) {
          builder.setIsNullable(true);
        } else {
          builder.setIsNullable(false);
        }
        builder.setDataType(column.DATA_TYPE);
        if (column.CHARACTER_MAXIMUM_LENGTH != null) {
          builder.setCharMaxLength(column.CHARACTER_MAXIMUM_LENGTH);
        }
        if (column.CHARACTER_OCTET_LENGTH != null) {
          builder.setCharOctetLength(column.CHARACTER_OCTET_LENGTH);
        }
        if (column.NUMERIC_SCALE != null) {
          builder.setNumericScale(column.NUMERIC_SCALE);
        }
        if (column.NUMERIC_PRECISION != null) {
          builder.setNumericPrecision(column.NUMERIC_PRECISION);
        }
        if (column.NUMERIC_PRECISION_RADIX != null) {
          builder.setNumericPrecisionRadix(column.NUMERIC_PRECISION_RADIX);
        }
        if (column.DATETIME_PRECISION != null) {
          builder.setDateTimePrecision(column.DATETIME_PRECISION);
        }
        if (column.INTERVAL_TYPE != null) {
          builder.setIntervalType(column.INTERVAL_TYPE);
        }
        if (column.INTERVAL_PRECISION != null) {
          builder.setIntervalPrecision(column.INTERVAL_PRECISION);
        }
        if (column.COLUMN_SIZE != null) {
          builder.setColumnSize(column.COLUMN_SIZE);
        }
        return builder.build();
      });
  }
}
