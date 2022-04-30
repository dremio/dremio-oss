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

import java.util.List;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

public class IcebergTableProps {

  private String tableLocation;
  private final String uuid;
  private BatchSchema fullSchema;
  private BatchSchema persistedFullSchema;
  private List<String> partitionColumnNames;
  private IcebergCommandType icebergOpType;
  private String tableName;
  private String dataTableLocation;
  private boolean detectSchema;
  private boolean isMetadataRefresh;
  private List<String> partitionPaths;
  private ResolvedVersionContext version;

  @JsonCreator
  public IcebergTableProps(
    @JsonProperty("tableLocation") String tableLocation,
    @JsonProperty("uuid") String uuid,
    @JsonProperty("fullSchema") BatchSchema fullSchema,
    @JsonProperty("partitionColumnNames") List<String> partitionColumnNames,
    @JsonProperty("icebergOpType") IcebergCommandType icebergOpType,
    @JsonProperty("tableName") String tableName,
    @JsonProperty("dataTableLocation") String dataTableLocation,
    @JsonProperty("versionContext") ResolvedVersionContext version) {
      this.tableLocation = tableLocation;
      this.uuid = uuid;
      this.fullSchema = fullSchema;
      this.partitionColumnNames = partitionColumnNames;
      this.icebergOpType = icebergOpType;
      this.tableName = tableName;
      this.dataTableLocation = dataTableLocation;
      this.version = version;
  }

  public IcebergTableProps(final IcebergTableProps other){
    this.partitionColumnNames = other.partitionColumnNames == null ? null : ImmutableList.copyOf(other.partitionColumnNames);
    this.tableLocation = other.tableLocation;
    this.uuid = other.uuid;
    this.fullSchema = other.fullSchema;
    this.persistedFullSchema = other.persistedFullSchema;
    this.icebergOpType = other.icebergOpType;
    this.tableName = other.tableName;
    this.detectSchema = other.detectSchema;
    this.isMetadataRefresh = other.isMetadataRefresh;
    this.partitionPaths = other.partitionPaths;
    this.version = other.version;
  }

  public String getTableLocation() {
    return tableLocation;
  }

  public String getUuid() {
    return uuid;
  }

  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  public BatchSchema getPersistedFullSchema() {
    return persistedFullSchema;
  }

  public IcebergCommandType getIcebergOpType() {
    return icebergOpType;
  }

  public void setFullSchema(BatchSchema schema) {
    this.fullSchema = schema;
  }

  public void setPersistedFullSchema(BatchSchema schema) {
    this.persistedFullSchema = schema;
  }

  public void setTableLocation(String tableFolder) {
    this.tableLocation = tableFolder;
  }

  public List<String> getPartitionColumnNames() {
    return partitionColumnNames;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public boolean isDetectSchema() {
    return detectSchema;
  }

  public void setDetectSchema(boolean detectSchema) {
    this.detectSchema = detectSchema;
  }

  public boolean isMetadataRefresh() {
    return isMetadataRefresh;
  }

  public void setMetadataRefresh(boolean metadataRefresh) {
    isMetadataRefresh = metadataRefresh;
  }

  public List<String> getPartitionPaths() {
    return partitionPaths;
  }

  public void setPartitionPaths(List<String> partitionPaths) {
    this.partitionPaths = partitionPaths;
  }

  public String getDataTableLocation() {
    return dataTableLocation;
  }

  public ResolvedVersionContext getVersion() {
    return version;
  }

  @JsonIgnore
  public boolean isSchemaSet() {
    return fullSchema != null;
  }
}
