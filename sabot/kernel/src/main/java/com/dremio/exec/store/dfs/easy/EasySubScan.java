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
package com.dremio.exec.store.dfs.easy;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.dremio.service.namespace.StoragePluginId;
import com.dremio.service.namespace.dataset.proto.DatasetSplit;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.protostuff.ByteString;

/**
 * Easy sub scan.
 */
@JsonTypeName("easy-sub-scan")
public class EasySubScan extends SubScanWithProjection {
  private final FileConfig fileConfig;
  private final List<DatasetSplit> splits;
  private final StoragePluginId pluginId;
  private final ByteString extendedProperty;
  private final List<String> partitionColumns;

  @JsonCreator
  public EasySubScan(
    @JsonProperty("settings") FileConfig config,
    @JsonProperty("splits") List<DatasetSplit> splits,
    @JsonProperty("userName") String userName,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("partitionColumns") List<String> partitionColumns,
    @JsonProperty("extendedProperty") ByteString extendedProperty) {
    super(userName, schema, tablePath, columns);
    this.fileConfig = config;
    this.splits = splits;
    this.pluginId = pluginId;
    this.extendedProperty = extendedProperty;
    this.partitionColumns = partitionColumns;
  }

  public List<DatasetSplit> getSplits() {
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

  public FileConfig getFileConfig() {
    return fileConfig;
  }

  @Override
  public int getOperatorType() {
    return EasyGroupScan.getEasyScanOperatorType(fileConfig.getType());
  }
}
