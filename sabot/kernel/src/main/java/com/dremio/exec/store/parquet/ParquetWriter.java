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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.StoragePluginRegistry;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FileSystemWriter;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("parquet-writer")
public class ParquetWriter extends FileSystemWriter {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ParquetWriter.class);

  private final String location;
  private final FileSystemPlugin plugin;
  private final ParquetFormatPlugin formatPlugin;

  @JsonCreator
  public ParquetWriter(
          @JsonProperty("child") PhysicalOperator child,
          @JsonProperty("userName") String userName,
          @JsonProperty("location") String location,
          @JsonProperty("options") WriterOptions options,
          @JsonProperty("storage") StoragePluginConfig storageConfig,
          @JacksonInject StoragePluginRegistry engineRegistry) throws IOException, ExecutionSetupException {

    super(child, userName, options);
    this.plugin = (FileSystemPlugin) engineRegistry.getPlugin(storageConfig);
    this.formatPlugin = (ParquetFormatPlugin) engineRegistry.getFormatPlugin(storageConfig, new ParquetFormatConfig());
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.location = location;
  }

  public ParquetWriter(
      PhysicalOperator child,
      String userName, String location,
      WriterOptions options,
      FileSystemPlugin plugin, ParquetFormatPlugin formatPlugin) {
    super(child, userName, options);
    this.plugin = plugin;
    this.formatPlugin = formatPlugin;
    this.location = location;
  }

  @JsonProperty("location")
  public String getLocation() {
    return location;
  }

  @JsonProperty("storage")
  public StoragePluginConfig getStorageConfig(){
    return formatPlugin.getStorageConfig();
  }

  @JsonIgnore
  public Configuration getFsConf() {
    return plugin.getFsConf();
  }

  @JsonIgnore
  public FormatPluginConfig getFormatConfig(){
    return formatPlugin.getConfig();
  }

  @JsonIgnore
  public ParquetFormatPlugin getFormatPlugin(){
    return formatPlugin;
  }

  @Override
  protected PhysicalOperator getNewWithChild(PhysicalOperator child) {
    return new ParquetWriter(child, getUserName(), location, getOptions(), plugin, formatPlugin);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_WRITER_VALUE;
  }

  @Override
  @JsonIgnore
  public boolean isPdfs() {
    return plugin.getFs().isPdfs();
  }
}
