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
package com.dremio.exec.store.parquet;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.physical.config.BoostPOP;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.planner.fragment.SplitNormalizer;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

/**
 * Parquet sub scan.
 */
@JsonTypeName("parquet-scan")
public class ParquetSubScan extends SubScanWithProjection {
  private final List<ParquetFilterCondition> conditions;
  private final StoragePluginId pluginId;
  private final FileConfig formatSettings;
  private final List<String> partitionColumns;
  private final List<List<String>> tablePath;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final ByteString extendedProperty;
  private final boolean arrowCachingEnabled;

  @JsonIgnore
  private List<SplitAndPartitionInfo> splits;

  public ParquetSubScan(
    OpProps props,
    FileConfig formatSettings,
    List<SplitAndPartitionInfo> splits,
    BatchSchema fullSchema,
    List<List<String>> tablePath,
    List<ParquetFilterCondition> conditions,
    StoragePluginId pluginId,
    List<SchemaPath> columns,
    List<String> partitionColumns,
    List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
    ByteString extendedProperty,
    boolean arrowCachingEnabled) {
    super(props, fullSchema, tablePath, columns);
    this.formatSettings = formatSettings;
    this.splits = splits;
    this.tablePath = tablePath;
    this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
    this.pluginId = pluginId;
    this.partitionColumns = partitionColumns;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.extendedProperty = extendedProperty;
    this.arrowCachingEnabled = arrowCachingEnabled;
  }

  @JsonCreator
  public ParquetSubScan(
    @JsonProperty("props") OpProps props,
    @JsonProperty("formatSettings") FileConfig formatSettings,
    @JsonProperty("schema") BatchSchema fullSchema,
    @JsonProperty("referencedTables") List<List<String>> tablePath,
    @JsonProperty("conditions") List<ParquetFilterCondition> conditions,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("globalDictionaryEncodedColumns") List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
    @JsonProperty("extendedProperty") ByteString extendedProperty,
    @JsonProperty("arrowCachingEnabled") boolean arrowCachingEnabled) {

    this(props, formatSettings, null, fullSchema, tablePath, conditions, pluginId, columns, partitionColumns,
      globalDictionaryEncodedColumns, extendedProperty, arrowCachingEnabled);
  }

  public FileConfig getFormatSettings(){
    return formatSettings;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<SplitAndPartitionInfo> getSplits() {
    return splits;
  }

  public List<List<String>> getTablePath() {
    return tablePath;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public List<ParquetFilterCondition> getConditions() {
    return conditions;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public List<GlobalDictionaryFieldInfo> getGlobalDictionaryEncodedColumns() {
    return globalDictionaryEncodedColumns;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  @JsonIgnore
  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    SplitNormalizer.write(getProps(), writer, splits);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    splits = SplitNormalizer.read(getProps(), reader);
  }

  @JsonIgnore
  @Override
  public BoostPOP getBoostConfig(List<SchemaPath> columnsToBoost) {
    return new BoostPOP(
      this.getProps(),
      this.getFormatSettings(),
      this.getSplits(),
      this.getFullSchema(),
      this.getTablePath(),
      this.getPluginId(),
      columnsToBoost,
      this.getPartitionColumns(),
      this.getGlobalDictionaryEncodedColumns(),
      this.getExtendedProperty());
  }
}
