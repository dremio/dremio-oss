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
package com.dremio.exec.store.dfs.system;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.SchemaConverter;
import org.apache.iceberg.Schema;

/**
 * {@code SystemIcebergViewMetadata} is an abstract class representing metadata for system iceberg
 * views. It contains information about the view's Iceberg schema, schema version, table name, and
 * methods to access or query the view.
 */
public abstract class SystemIcebergViewMetadata {

  /** The Iceberg schema representing the view's structure. */
  private final Schema schema;

  /** The version of the schema for the view. */
  private final long schemaVersion;

  /** The name of the table associated with the view. */
  private final String tableName;

  /**
   * Constructs a new {@code SystemIcebergViewMetadata} with the specified schema, schema version,
   * and table name.
   *
   * @param schema The Iceberg {@link Schema} representing the view's schema.
   * @param schemaVersion The version of the schema for the view.
   * @param tableName The name of the table associated with the view.
   */
  public SystemIcebergViewMetadata(Schema schema, long schemaVersion, String tableName) {
    this.schema = schema;
    this.tableName = tableName;
    this.schemaVersion = schemaVersion;
  }

  /**
   * Get the Iceberg schema for the view.
   *
   * @return The Iceberg {@link Schema} representing the schema of the view.
   */
  public Schema getIcebergSchema() {
    return schema;
  }

  /**
   * Get the BatchSchema representation of the view's schema.
   *
   * @return The BatchSchema object representing the schema of the view.
   */
  public BatchSchema getBatchSchema() {
    return SchemaConverter.getBuilder()
        .setTableName(tableName)
        .build()
        .fromIceberg(getIcebergSchema());
  }

  /**
   * Get the name of the table associated with the view.
   *
   * @return The name of the table.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get the schema version of the view.
   *
   * @return The schema version.
   */
  public long getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * Get a view query for the view based on the provided user name.
   *
   * @param userName The user name for whom the query is generated.
   * @return The SQL query for the view.
   */
  public String getViewQuery(String userName) {
    return new SystemIcebergViewQueryBuilder(getSchemaVersion(), getTableName(), null, userName)
        .getViewQuery();
  }
}
