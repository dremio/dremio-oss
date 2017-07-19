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
import com.dremio.service.namespace.dataset.proto.ReadDefinition;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Easy sub scan.
 */
@JsonTypeName("easy-sub-scan")
public class EasySubScan extends SubScanWithProjection {
  private final ReadDefinition definition;
  private final FileConfig fileConfig;
  private final List<DatasetSplit> splits;
  private final StoragePluginId pluginId;

  @JsonCreator
  public EasySubScan(
    @JsonProperty("readDefinition") ReadDefinition definition,
    @JsonProperty("settings") FileConfig config,
    @JsonProperty("splits") List<DatasetSplit> splits,
    @JsonProperty("userName") String userName,
    @JsonProperty("schema") BatchSchema schema,
    @JsonProperty("tableSchemaPath") List<String> tablePath,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns) {
    super(userName, schema, tablePath, columns);
    this.definition = definition;
    this.fileConfig = config;
    this.splits = splits;
    this.pluginId = pluginId;
  }

  public ReadDefinition getReadDefinition() {
    return definition;
  }

  public List<DatasetSplit> getSplits() {
    return splits;
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
