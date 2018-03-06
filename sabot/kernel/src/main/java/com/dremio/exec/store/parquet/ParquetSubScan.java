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

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
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

  private final List<DatasetSplit> splits;
  private final List<FilterCondition> conditions;
  private final StoragePluginId pluginId;
  private final FileConfig formatSettings;
  private final List<String> partitionColumns;
  private final List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns;
  private final ByteString extendedProperty;

  @JsonCreator
  public ParquetSubScan(
    @JsonProperty("formatSettings") FileConfig formatSettings,
    @JsonProperty("splits") List<DatasetSplit> splits,
    @JsonProperty("userName") String userName,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("conditions") List<FilterCondition> conditions,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("globalDictionaryEncodedColumns") List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
    @JsonProperty("extendedProperty") ByteString extendedProperty) {
    super(userName, schema, tablePath, columns);
    this.formatSettings = formatSettings;
    this.splits = splits;
    this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
    this.pluginId = pluginId;
    this.partitionColumns = partitionColumns;
    this.globalDictionaryEncodedColumns = globalDictionaryEncodedColumns;
    this.extendedProperty = extendedProperty;
  }

  public FileConfig getFormatSettings(){
    return formatSettings;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public List<FilterCondition> getConditions() {
    return conditions;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public List<GlobalDictionaryFieldInfo> getGlobalDictionaryEncodedColumns() {
    return globalDictionaryEncodedColumns;
  }

  @JsonIgnore
  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.PARQUET_ROW_GROUP_SCAN_VALUE;
  }
}
