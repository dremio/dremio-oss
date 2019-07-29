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
package com.dremio.exec.store.dfs;

import java.io.IOException;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.MutablePlugin;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.store.CatalogService;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Implements <code>CreateTableEntry</code> interface to create new tables in arbitrary storage.
 */
@JsonTypeName("generic")
public class GenericCreateTableEntry implements CreateTableEntry {

  private final String userName;
  private final MutablePlugin plugin;
  private final String location;
  private final WriterOptions options;

  @JsonCreator
  public GenericCreateTableEntry(
      @JsonProperty("userName") String userName,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("location") String location,
      @JsonProperty("options") WriterOptions options,
      @JacksonInject CatalogService catalogService)
      throws ExecutionSetupException {
    this.userName = userName;
    this.plugin = catalogService.getSource(pluginId);
    this.location = location;
    this.options = options;
  }

  /**
   * Create an instance.
   *
   * @param userName Name of the user whom to impersonate while creating the table
   * @param plugin {@link FileSystemPlugin} instance
   * @param formatPlugin Reference to the {@link FormatPlugin} for output type
   * @param location Output path
   */
  public GenericCreateTableEntry(
      String userName,
      MutablePlugin plugin,
      String location,
      WriterOptions options) {
    this.userName = userName;
    this.plugin = plugin;
    this.location = location;
    this.options = options;
  }

  @JsonProperty("pluginId")
  public StoragePluginId getId() {
    return plugin.getId();
  }

  public String getLocation(){
    return location;
  }

  public String getUserName() {
    return userName;
  }

  @JsonIgnore
  public MutablePlugin getPlugin(){
    return plugin;
  }

  public WriterOptions getOptions() {
    return options;
  }

  @Override
  public Writer getWriter(OpProps props, PhysicalOperator child) throws IOException {
    return plugin.getWriter(child, location, options, props);
  }
}
