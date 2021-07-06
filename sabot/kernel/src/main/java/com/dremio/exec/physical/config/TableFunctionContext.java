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
package com.dremio.exec.physical.config;

import java.util.Collection;
import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.protostuff.ByteString;


/**
 * Table function context
 */
@JsonTypeName("table-function-context")
public class TableFunctionContext {
  private final List<SchemaPath> columns;
  private final ScanFilter scanFilter;
  private final StoragePluginId pluginId;
  private final StoragePluginId internalTablePluginId;
  private final FileConfig formatSettings;
  private final List<String> partitionColumns;
  private final List<List<String>> tablePath;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final ByteString extendedProperty;
  private final boolean arrowCachingEnabled;
  private final BatchSchema fullSchema;
  private final BatchSchema tableSchema;
  private final Collection<List<String>> referencedTables;

  public TableFunctionContext(@JsonProperty("formatSettings") FileConfig formatSettings,
                              @JsonProperty("schema") BatchSchema fullSchema,
                              @JsonProperty("tableschema") BatchSchema tableSchema,
                              @JsonProperty("referencedTables") List<List<String>> tablePath,
                              @JsonProperty("scanFilter") ScanFilter scanFilter,
                              @JsonProperty("pluginId") StoragePluginId pluginId,
                              @JsonProperty("internalTablePluginId") StoragePluginId internalTablePluginId,
                              @JsonProperty("columns") List<SchemaPath> columns,
                              @JsonProperty("partitionColumns") List<String> partitionColumns,
                              @JsonProperty("globalDictionaryEncodedColumns") List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
                              @JsonProperty("extendedProperty") ByteString extendedProperty,
                              @JsonProperty("arrowCachingEnabled") boolean arrowCachingEnabled) {
    this.fullSchema = fullSchema;
    this.tableSchema = tableSchema;
    this.referencedTables = tablePath;
    this.columns = columns;
    this.formatSettings = formatSettings;
    this.tablePath = tablePath;
    this.scanFilter = scanFilter;
    this.pluginId = pluginId;
    this.internalTablePluginId = internalTablePluginId;
    this.partitionColumns = partitionColumns;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.extendedProperty = extendedProperty;
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  public FileConfig getFormatSettings(){
    return formatSettings;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<List<String>> getTablePath() {
    return tablePath;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public ScanFilter getScanFilter() {
    return scanFilter;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public StoragePluginId getInternalTablePluginId() {
    return internalTablePluginId;
  }

  public List<GlobalDictionaryFieldInfo> getGlobalDictionaryEncodedColumns() {
    return globalDictionaryEncodedColumns;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @JsonProperty("fullSchema")
  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  @JsonProperty("tableSchema")
  public BatchSchema getTableSchema() {
    return tableSchema;
  }

  public Collection<List<String>> getReferencedTables() {
    return referencedTables;
  }
}
