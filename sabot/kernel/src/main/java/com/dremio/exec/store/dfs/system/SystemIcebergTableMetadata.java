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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.io.file.Path;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * This class represents the metadata for the tables stored by {@link SystemIcebergTablesStoragePlugin}
 * It provides methods to access the schema, batch schema, table location, and other
 * properties of the table.
 * */
public abstract class SystemIcebergTableMetadata {

  private final Schema schema;
  private final long schemaVersion;
  private final NamespaceKey namespaceKey;
  private final String tableLocation;
  private final String tableName;

  /**
   * Constructs an instance of SystemIcebergTableMetadata.
   *
   * @param schemaVersion The version of the schema for the table.
   * @param schema        The iceberg schema of the table.
   * @param pluginName    Name of the {@link SystemIcebergTablesStoragePlugin}
   * @param pluginPath    Path of the {@link SystemIcebergTablesStoragePlugin}
   * @param tableName     Name of the iceberg table.
   */
  public SystemIcebergTableMetadata(long schemaVersion, Schema schema, String pluginName, String pluginPath, String tableName) {
    this.schema = schema;
    this.namespaceKey = new NamespaceKey(ImmutableList.of(pluginName, tableName));
    this.tableLocation = Path.of(pluginPath).resolve(namespaceKey.getName()).toString();
    this.tableName = tableName;
    this.schemaVersion = schemaVersion;
  }

  /**
   * Returns the name of the table.
   *
   * @return The name of the table.
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Get the Iceberg schema for the table.
   *
   * @return The Iceberg Schema object representing the schema of the table.
   */
  public Schema getIcebergSchema() {
    return schema;
  }

  /**
   * Get the BatchSchema representation of the table schema.
   *
   * @return The BatchSchema object representing the schema of the table.
   */
  public BatchSchema getBatchSchema() {
    return SchemaConverter.getBuilder().setTableName(tableName).build().fromIceberg(getIcebergSchema());
  }

  /**
   * Get the location of the table.
   *
   * @return The string representing the location of the table.
   */
  public String getTableLocation() {
    return tableLocation;
  }

  /**
   * Get the NamespaceKey for the table.
   *
   * @return The NamespaceKey object representing the namespace path of the table.
   */
  public NamespaceKey getNamespaceKey() {
    return this.namespaceKey;
  }

  /**
   * Get the current schema version of the underlying table.
   *
   * @return current schema version of the table
   */
  public long getSchemaVersion() {
    return schemaVersion;
  }

  /**
   * Get the IcebergTableProps object required for creating the table in Iceberg.
   *
   * @return The IcebergTableProps object representing the properties of the table for the CREATE operation.
   */
  public IcebergTableProps getIcebergTablePropsForCreate() {
    TableMetadata tableMetadata = TableMetadata.buildFromEmpty()
      .addSchema(getIcebergSchema(), getIcebergSchema().highestFieldId())
      .assignUUID()
      .addPartitionSpec(PartitionSpec.unpartitioned())
      .addSortOrder(SortOrder.unsorted())
      .setLocation(getTableLocation())
      .setProperties(ImmutableMap.of("schema_version", String.valueOf(schemaVersion)))
      .build();
    return IcebergTableProps.createInstance(IcebergCommandType.CREATE, null, null, tableMetadata, null);
  }

  /**
   * Get the IcebergTableProps object required for inserting into the table in Iceberg.
   *
   * @return The IcebergTableProps object representing the properties of the table for the INSERT operation.
   */
  public IcebergTableProps getIcebergTablePropsForInsert() {
    TableMetadata tableMetadata = TableMetadata.buildFromEmpty()
      .addSchema(getIcebergSchema(), getIcebergSchema().highestFieldId())
      .addPartitionSpec(PartitionSpec.unpartitioned())
      .addSortOrder(SortOrder.unsorted())
      .setLocation(getTableLocation())
      .build();
    return IcebergTableProps.createInstance(IcebergCommandType.INSERT, null, null, tableMetadata, null);
  }


  /**
   * Generates a view query based on the schema version, table name, and namespace key of this dataset handle.
   * This method constructs a view query by calling the {@link SystemIcebergViewQueryBuilder#getViewQuery()} method
   * with the current schema version, table name, and namespace key, and a null username.
   *
   * @return The generated view query as a SQL string.
   * @throws UnsupportedOperationException if the schema version or table name is not supported, or if the generation of
   * the view query is not possible for the given parameters.
   */
  public String getViewQuery() {
    return new SystemIcebergViewQueryBuilder(getSchemaVersion(), getTableName(), getNamespaceKey(), null).getViewQuery();
  }
}
