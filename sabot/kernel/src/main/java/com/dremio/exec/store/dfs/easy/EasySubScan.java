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
package com.dremio.exec.store.dfs.easy;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.planner.fragment.MinorDataReader;
import com.dremio.exec.planner.fragment.MinorDataWriter;
import com.dremio.exec.planner.fragment.SplitNormalizer;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.exec.store.iceberg.IcebergExtendedProp;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.protostuff.ByteString;
import java.util.List;

/** Easy sub scan. */
@JsonTypeName("easy-sub-scan")
public class EasySubScan extends SubScanWithProjection {
  private static final String SPLITS_ATTRIBUTE_KEY = "easy-sub-scan-splits";

  private final FileConfig fileConfig;
  private final StoragePluginId pluginId;
  private UserDefinedSchemaSettings userDefinedSchemaSettings;
  private StoragePluginId datasourcePluginId;
  private final ByteString extendedProperty;
  private final List<String> partitionColumns;
  private final IcebergExtendedProp icebergExtendedProp;

  @JsonIgnore private List<SplitAndPartitionInfo> splits;

  public EasySubScan(
      OpProps props,
      FileConfig config,
      List<SplitAndPartitionInfo> splits,
      BatchSchema fullSchema,
      List<String> tablePath,
      StoragePluginId pluginId,
      StoragePluginId datasourcePluginId,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProperty,
      IcebergExtendedProp icebergExtendedProp,
      UserDefinedSchemaSettings userDefinedSchemaSettings) {
    super(props, fullSchema, (tablePath == null) ? null : ImmutableList.of(tablePath), columns);
    this.fileConfig = config;
    this.splits = splits;
    this.pluginId = pluginId;
    this.datasourcePluginId = datasourcePluginId;
    this.extendedProperty = extendedProperty;
    this.partitionColumns = partitionColumns;
    this.icebergExtendedProp = icebergExtendedProp;
    this.userDefinedSchemaSettings = userDefinedSchemaSettings;
  }

  public EasySubScan(
      OpProps props,
      FileConfig config,
      List<SplitAndPartitionInfo> splits,
      BatchSchema fullSchema,
      List<String> tablePath,
      StoragePluginId pluginId,
      StoragePluginId datasourcePluginId,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProperty,
      IcebergExtendedProp icebergExtendedProp) {
    this(
        props,
        config,
        splits,
        fullSchema,
        tablePath,
        pluginId,
        datasourcePluginId,
        columns,
        partitionColumns,
        extendedProperty,
        icebergExtendedProp,
        null);
  }

  @JsonCreator
  public EasySubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("settings") FileConfig config,
      @JsonProperty("fullSchema") BatchSchema fullSchema,
      @JsonProperty("tableSchemaPath") List<String> tablePath,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("datasourcePluginId") StoragePluginId datasourcePluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("extendedProperty") ByteString extendedProperty,
      @JsonProperty("icebergExtendedProperties") IcebergExtendedProp icebergExtendedProp,
      @JsonProperty("userDefinedSchemaSettings")
          UserDefinedSchemaSettings userDefinedSchemaSettings) {

    this(
        props,
        config,
        null,
        fullSchema,
        tablePath,
        pluginId,
        datasourcePluginId,
        columns,
        partitionColumns,
        extendedProperty,
        icebergExtendedProp,
        userDefinedSchemaSettings);
  }

  public List<SplitAndPartitionInfo> getSplits() {
    return splits;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public StoragePluginId getDatasourcePluginId() {
    return datasourcePluginId;
  }

  public FileConfig getFileConfig() {
    return fileConfig;
  }

  public IcebergExtendedProp getIcebergExtendedProp() {
    return icebergExtendedProp;
  }

  @Override
  public int getOperatorType() {
    return EasyGroupScan.getEasyScanOperatorType(fileConfig.getType());
  }

  @Override
  public void collectMinorSpecificAttrs(MinorDataWriter writer) {
    SplitNormalizer.write(getProps(), writer, splits);
  }

  @Override
  public void populateMinorSpecificAttrs(MinorDataReader reader) throws Exception {
    splits = SplitNormalizer.read(getProps(), reader);
  }

  public UserDefinedSchemaSettings getUserDefinedSchemaSettings() {
    return userDefinedSchemaSettings;
  }
}
