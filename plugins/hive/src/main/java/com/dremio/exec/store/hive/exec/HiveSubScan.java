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
package com.dremio.exec.store.hive.exec;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.planner.fragment.SplitNormalizer;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

@JsonTypeName("hive-sub-scan")
public class HiveSubScan extends SubScanWithProjection {

  private final ScanFilter filter;
  private final StoragePluginId pluginId;
  private final List<String> partitionColumns;
  private final byte[] extendedProperty;

  @JsonIgnore
  private List<SplitAndPartitionInfo> splits;

  public HiveSubScan(
    OpProps props,
    List<SplitAndPartitionInfo> splits,
    BatchSchema fullSchema,
    List<String> tablePath,
    ScanFilter filter,
    StoragePluginId pluginId,
    List<SchemaPath> columns,
    List<String> partitionColumns,
    byte[] extendedProperty
  ) {
    super(props, fullSchema, tablePath, columns);
    this.splits = splits;
    this.filter = filter;
    this.pluginId = pluginId;
    this.partitionColumns = partitionColumns != null ? ImmutableList.copyOf(partitionColumns) : null;
    this.extendedProperty = extendedProperty;
  }

  @JsonCreator
  public HiveSubScan(
    @JsonProperty("props") OpProps props,
    @JsonProperty("fullSchema") BatchSchema fullSchema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("filter") ScanFilter filter,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("extendedProperty") byte[] extendedProperty) {
    this(props, null, fullSchema, tablePath, filter, pluginId, columns, partitionColumns, extendedProperty);
  }

  public StoragePluginId getPluginId(){
    return pluginId;
  }

  public ScanFilter getFilter(){
    return filter;
  }

  public List<SplitAndPartitionInfo> getSplits() {
    return splits;
  }

  public byte[] getExtendedProperty() { return this.extendedProperty; }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.HIVE_SUB_SCAN_VALUE;
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    SplitNormalizer.write(getProps(), writer, splits);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    splits = SplitNormalizer.read(getProps(), reader);
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }
}
