/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import org.apache.hadoop.security.UserGroupInformation;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.proto.UserBitShared.CoreOperatorType;
import com.dremio.exec.store.CatalogService;
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
      @JsonProperty("props") OpProps props,
      @JsonProperty("child") PhysicalOperator child,
      @JsonProperty("location") String location,
      @JsonProperty("options") WriterOptions options,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JacksonInject CatalogService catalogService) throws IOException, ExecutionSetupException {
    super(props, child, options);
    this.plugin = catalogService.getSource(pluginId);
    this.formatPlugin = (ParquetFormatPlugin) plugin.getFormatPlugin(new ParquetFormatConfig());
    Preconditions.checkNotNull(formatPlugin, "Unable to load format plugin for provided format config.");
    this.location = location;
  }

  public ParquetWriter(
      OpProps props,
      PhysicalOperator child,
      String location,
      WriterOptions options,
      FileSystemPlugin plugin,
      ParquetFormatPlugin formatPlugin) {
    super(props, child, options);
    this.plugin = plugin;
    this.formatPlugin = formatPlugin;
    this.location = location;
  }

  public StoragePluginId getPluginId() {
    return plugin.getId();
  }

  @JsonProperty("location")
  public String getLocation() {
    return location;
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
    return new ParquetWriter(props, child, location, getOptions(), plugin, formatPlugin);
  }

  @Override
  public int getOperatorType() {
    return CoreOperatorType.PARQUET_WRITER_VALUE;
  }

  @Override
  @JsonIgnore
  public boolean isPdfs() {
    return plugin.getSystemUserFS().isPdfs();
  }

  @JsonIgnore
  public UserGroupInformation getUGI() {
    return formatPlugin.getFsPlugin().getUGIForUser(props.getUserName());
  }
}
