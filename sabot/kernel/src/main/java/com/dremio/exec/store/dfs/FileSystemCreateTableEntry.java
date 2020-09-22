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

import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.WriterPrel;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.SchemaBuilder;
import com.dremio.exec.store.CatalogService;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Implements <code>CreateTableEntry</code> interface to create new tables in FileSystem storage.
 */
@JsonTypeName("filesystem")
public class FileSystemCreateTableEntry implements CreateTableEntry {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FileSystemCreateTableEntry.class);

  private final String userName;
  private final FileSystemPlugin plugin;
  private final FormatPlugin formatPlugin;
  private final String location;
  private final WriterOptions options;
  private final IcebergTableProps icebergTableProps;

  @JsonCreator
  public FileSystemCreateTableEntry(@JsonProperty("userName") String userName,
                                    @JsonProperty("pluginId") StoragePluginId pluginId,
                                    @JsonProperty("formatConfig") FormatPluginConfig formatConfig,
                                    @JsonProperty("location") String location,
                                    @JsonProperty("icebergTableProps") IcebergTableProps icebergTableProps,
                                    @JsonProperty("options") WriterOptions options,
                                    @JacksonInject CatalogService catalogService)
      throws ExecutionSetupException {
    this.userName = userName;
    this.plugin = catalogService.getSource(pluginId);
    this.formatPlugin = plugin.getFormatPlugin(formatConfig);
    this.location = location;
    this.options = options;
    this.icebergTableProps = icebergTableProps;
  }

  /**
   * Create an instance.
   *
   * @param userName Name of the user whom to impersonate while creating the table
   * @param plugin {@link FileSystemPlugin} instance
   * @param formatPlugin Reference to the {@link FormatPlugin} for output type
   * @param location Output path
   */
  public FileSystemCreateTableEntry(
      String userName,
      FileSystemPlugin plugin,
      FormatPlugin formatPlugin,
      String location,
      IcebergTableProps icebergTableProps,
      WriterOptions options) {
    this.userName = userName;
    this.plugin = plugin;
    this.formatPlugin = formatPlugin;
    this.location = location;
    this.options = options;
    this.icebergTableProps = icebergTableProps;
  }

  @JsonProperty("pluginId")
  public StoragePluginId getId() {
    return plugin.getId();
  }

  @JsonProperty("formatConfig")
  public FormatPluginConfig getFormatConfig() {
    return formatPlugin.getConfig();
  }

  @JsonProperty("location")
  public String getLocation(){
    return location;
  }

  @JsonIgnore
  public FileSystemPlugin getPlugin(){
    return plugin;
  }

  public FileSystemCreateTableEntry cloneWithNewLocation(String newLocation){
    return new FileSystemCreateTableEntry(userName, plugin, formatPlugin, newLocation, icebergTableProps, options);
  }

  public String getUserName() {
    return userName;
  }

  @Override
  public Writer getWriter(
      OpProps props,
      PhysicalOperator child
      ) throws IOException {
    if (child != null && child.getProps() != null && icebergTableProps != null) {
      BatchSchema writerSchema = child.getProps().getSchema();
      SchemaBuilder schemaBuilder = BatchSchema.newBuilder();
      // current parquet writer uses a few extra columns in the schema for partitioning and distribution
      // For iceberg, filter those extra columns
      for (Field field : writerSchema) {
        if (field.getName().equalsIgnoreCase(WriterPrel.PARTITION_COMPARATOR_FIELD)) {
          continue;
        }
        if (field.getName().equalsIgnoreCase(WriterPrel.BUCKET_NUMBER_FIELD)) {
          continue;
        }
        schemaBuilder.addField(field);
      }
      writerSchema = schemaBuilder.build();
      icebergTableProps.setFullSchema(writerSchema);
    }
    return formatPlugin.getWriter(child, location, plugin, options, props);
  }

  public WriterOptions getOptions() {
    return options;
  }

  public IcebergTableProps getIcebergTableProps() {
    return icebergTableProps;
  }

}
