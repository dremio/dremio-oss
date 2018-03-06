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
package com.dremio.exec.store.hive.exec;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.parquet.FilterCondition;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

@JsonTypeName("hive-sub-scan")
public class HiveSubScan extends SubScanWithProjection {

  private final List<DatasetSplit> splits;
  private final List<FilterCondition> conditions;
  private final StoragePluginId pluginId;
  private final ByteString extendedProperty;
  private final List<String> partitionColumns;

  @JsonCreator
  public HiveSubScan(
      @JsonProperty("splits") List<DatasetSplit> splits,
      @JsonProperty("userName") String userName,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("tableSchemaPath") List<String> tablePath,
      @JsonProperty("conditions") List<FilterCondition> conditions,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("extendedProperty") ByteString extendedProperty,
      @JsonProperty("partitionColumns") List<String> partitionColumns
      ) {
    super(userName, schema, tablePath, columns);
    this.splits = splits;
    this.conditions = conditions == null ? null : ImmutableList.copyOf(conditions);
    this.pluginId = pluginId;
    this.extendedProperty = extendedProperty;
    this.partitionColumns = partitionColumns != null ? ImmutableList.copyOf(partitionColumns) : null;
  }

  public StoragePluginId getPluginId(){
    return pluginId;
  }

  public List<FilterCondition> getConditions(){
    return conditions;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }

}
