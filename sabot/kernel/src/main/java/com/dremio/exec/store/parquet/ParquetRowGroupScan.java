/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.parquet;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

// Class containing information for reading a single parquet row group form HDFS
@JsonTypeName("parquet-row-group-scan")
public class ParquetRowGroupScan extends SubScanWithProjection {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetRowGroupScan.class);

  private final FileSystemPlugin plugin;
  private final ParquetFormatPlugin formatPlugin;
  private final List<RowGroupReadEntry> rowGroupReadEntries;
  private final List<SchemaPath> columns;
  private final String selectionRoot;
  private final boolean includeModTime;
  private final Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns;
  private final List<FilterCondition> conditions;

  @JsonCreator
  public ParquetRowGroupScan(
      @JacksonInject StoragePluginRegistry registry,
      @JsonProperty("userName") String userName,
      @JsonProperty("storage") StoragePluginConfig storageConfig,
      @JsonProperty("format") FormatPluginConfig formatConfig,
      @JsonProperty("entries") LinkedList<RowGroupReadEntry> rowGroupReadEntries,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("selectionRoot") String selectionRoot,
      @JsonProperty("tableSchemaPath") List<String> tableSchemaPath,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("includeModTime") boolean includeModTime,
      @JsonProperty("globalDictionaryColumns") Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns,
      @JsonProperty("conditions") List<FilterCondition> conditions
  ) throws ExecutionSetupException {
    this(
        userName,
        (FileSystemPlugin)registry.getPlugin(storageConfig),
        (ParquetFormatPlugin) registry.getFormatPlugin(
            Preconditions.checkNotNull(storageConfig),
            formatConfig == null ? new ParquetFormatConfig() : formatConfig),
        rowGroupReadEntries,
        columns,
        selectionRoot,
        tableSchemaPath,
        schema,
        includeModTime,
        globalDictionaryColumns,
        conditions);
  }

  public ParquetRowGroupScan(
      String userName,
      FileSystemPlugin plugin,
      ParquetFormatPlugin formatPlugin,
      List<RowGroupReadEntry> rowGroupReadEntries,
      List<SchemaPath> columns,
      String selectionRoot,
      List<String> tableSchemaPath,
      BatchSchema schema,
      boolean includeModTime,
      Map<String, GlobalDictionaryFieldInfo> globalDictionaryColumns,
      List<FilterCondition> conditions
  ) {
    super(userName, schema, tableSchemaPath, columns);
    this.formatPlugin = Preconditions.checkNotNull(formatPlugin);
    this.plugin = plugin;
    this.rowGroupReadEntries = rowGroupReadEntries;
    this.columns = columns == null ? GroupScan.ALL_COLUMNS : columns;
    this.selectionRoot = selectionRoot;
    this.includeModTime = includeModTime;
    this.globalDictionaryColumns = globalDictionaryColumns == null? Collections.<String, GlobalDictionaryFieldInfo>emptyMap() : globalDictionaryColumns;
    this.conditions = conditions;
  }

  @JsonProperty("entries")
  public List<RowGroupReadEntry> getRowGroupReadEntries() {
    return rowGroupReadEntries;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getEngineConfig() {
    return formatPlugin.getStorageConfig();
  }

  @JsonProperty("includeModTime")
  public boolean includeModTime() {
    return includeModTime;
  }

  public String getSelectionRoot() {
    return selectionRoot;
  }

  public Map<String, GlobalDictionaryFieldInfo> getGlobalDictionaryColumns() {
    return globalDictionaryColumns;
  }

  @JsonIgnore
  public FileSystemPlugin getStorageEngine() {
    return plugin;
  }

  public FormatPluginConfig getFormat(){
    return formatPlugin.getConfig();
  }

  public List<FilterCondition> getConditions() {
    return conditions;
  }

  @JsonIgnore
  public ParquetFormatPlugin getFormatPlugin() {
    return formatPlugin;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value) throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) throws ExecutionSetupException {
    Preconditions.checkArgument(children.isEmpty());
    return new ParquetRowGroupScan(getUserName(), plugin, formatPlugin, rowGroupReadEntries, columns, selectionRoot, getTableSchemaPath(), getSchema(), includeModTime, globalDictionaryColumns, conditions);
  }

  @Override
  public Iterator<PhysicalOperator> iterator() {
    return Iterators.emptyIterator();
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

}
