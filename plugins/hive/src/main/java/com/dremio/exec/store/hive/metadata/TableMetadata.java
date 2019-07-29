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
package com.dremio.exec.store.hive.metadata;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.hadoop.hive.metastore.api.Table;

import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.hive.HiveStoragePlugin;
import com.dremio.hive.proto.HiveReaderProto.ColumnInfo;

/**
 * Helper class to hold table metadata details used in {@link HiveStoragePlugin#getDatasetMetadata}
 * and {@link HiveStoragePlugin#listPartitionChunks}.
 */
public class TableMetadata {

  private final Table table;

  private final Properties tableProperties;
  private final BatchSchema batchSchema;
  private final List<Field> fields;
  private final List<String> partitionColumns;
  private final List<ColumnInfo> columnInfos;

  private final HiveStorageCapabilities tableStorageCapabilities;

  private TableMetadata(final Table table, final Properties tableProperties, final BatchSchema batchSchema,
                        final List<Field> fields, final List<String> partitionColumns,
                        final List<ColumnInfo> columnInfos) {
    this.table = table;
    this.tableProperties = tableProperties;
    this.batchSchema = batchSchema;
    this.fields = fields;
    this.partitionColumns = partitionColumns;
    this.columnInfos = columnInfos;

    this.tableStorageCapabilities = HiveMetadataUtils.getHiveStorageCapabilities(table.getSd());
  }

  public Table getTable() {
    return table;
  }

  public Properties getTableProperties() {
    return tableProperties;
  }

  public BatchSchema getBatchSchema() {
    return batchSchema;
  }

  public List<Field> getFields() {
    return fields;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<ColumnInfo> getColumnInfos() {
    return columnInfos;
  }

  public HiveStorageCapabilities getTableStorageCapabilities() {
    return tableStorageCapabilities;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {
    private Table table;
    private Properties tableProperties;
    private BatchSchema batchSchema;
    private List<Field> fields;
    private List<String> partitionColumns;
    private List<ColumnInfo> columnInfos;

    private Builder() {
    }

    public Builder table(Table table) {
      this.table = table;
      return this;
    }

    public Builder tableProperties(Properties tableProperties) {
      this.tableProperties = tableProperties;
      return this;
    }

    public Builder batchSchema(BatchSchema batchSchema) {
      this.batchSchema = batchSchema;
      return this;
    }

    public Builder fields(List<Field> fields) {
      this.fields = fields;
      return this;
    }

    public Builder partitionColumns(List<String> partitionColumns) {
      this.partitionColumns = partitionColumns;
      return this;
    }

    public Builder columnInfos(List<ColumnInfo> columnInfos) {
      this.columnInfos = columnInfos;
      return this;
    }

    public TableMetadata build() {

      Objects.requireNonNull(table, "table is required");
      Objects.requireNonNull(tableProperties, "table properties id is required");
      Objects.requireNonNull(batchSchema, "batch schema is required");
      Objects.requireNonNull(fields, "fields is required");
      Objects.requireNonNull(partitionColumns, "partition columns is required");
      Objects.requireNonNull(columnInfos, "column infos is required");

      return new TableMetadata(table, tableProperties, batchSchema, fields, partitionColumns, columnInfos);
    }
  }
}
