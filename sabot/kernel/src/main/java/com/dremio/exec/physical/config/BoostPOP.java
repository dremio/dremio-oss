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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.parquet.ParquetSubScan;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.protostuff.ByteString;
import java.util.Collections;
import java.util.List;

@JsonTypeName("boost-parquet")
public class BoostPOP extends SubScanWithProjection {

  private final StoragePluginId pluginId;
  private final FileConfig formatSettings;
  private final List<String> partitionColumns;
  private final List<List<String>> tablePath;
  private final ByteString extendedProperty;
  private final List<SplitAndPartitionInfo> splits;
  private final boolean unlimitedSplitsBoost;

  @JsonCreator
  public BoostPOP(
      @JsonProperty("props") OpProps props,
      @JsonProperty("formatSettings") FileConfig formatSettings,
      @JsonProperty("splits") List<SplitAndPartitionInfo> splits,
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("referencedTables") List<List<String>> tablePath,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("extendedProperty") ByteString extendedProperty,
      @JsonProperty("unlimitedSplitsBoost") boolean unlimitedSplitsBoost) {
    super(props, fullSchema, tablePath, columns);
    this.formatSettings = formatSettings;
    this.splits = splits;
    this.tablePath = tablePath;
    this.pluginId = pluginId;
    this.partitionColumns = partitionColumns;
    this.extendedProperty = extendedProperty;
    this.unlimitedSplitsBoost = unlimitedSplitsBoost;
  }

  @Override
  public boolean mayLearnSchema() {
    return false;
  }

  public FileConfig getFormatSettings() {
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

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public boolean isUnlimitedSplitsBoost() {
    return unlimitedSplitsBoost;
  }

  @JsonIgnore
  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.BOOST_PARQUET_VALUE;
  }

  public ParquetSubScan asParquetSubScan() {
    return new ParquetSubScan(
        props,
        formatSettings,
        splits,
        getFullSchema(),
        tablePath,
        null,
        pluginId,
        getColumns(),
        partitionColumns,
        extendedProperty,
        true);
  }

  public BoostPOP withEmptyColumnsToBoost() {
    return new BoostPOP(
        this.props,
        this.formatSettings,
        this.splits,
        getFullSchema(),
        tablePath,
        pluginId,
        Collections.emptyList(),
        partitionColumns,
        extendedProperty,
        unlimitedSplitsBoost);
  }
}
