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

import static com.dremio.exec.store.iceberg.IcebergSerDe.deserializedJsonAsSchema;

import java.util.List;

import org.apache.iceberg.PartitionSpec;

import com.dremio.exec.catalog.ResolvedVersionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

public class IcebergTableProps {

  private String tableLocation;
  private final String uuid;
  private BatchSchema fullSchema;
  private BatchSchema persistedFullSchema;
  private List<String> partitionColumnNames;
  private String tableName;
  private String dataTableLocation;
  private List<String> partitionPaths;
  private ResolvedVersionContext version;
  private String databaseName;
  private ByteString partitionSpec;
  private String icebergSchema;

  // TODO: Separate action specific props from "TableProperties"
  private boolean detectSchema;
  private boolean isMetadataRefresh;
  private IcebergCommandType icebergOpType;

  @JsonCreator
  public IcebergTableProps(
    @JsonProperty("tableLocation") String tableLocation,
    @JsonProperty("uuid") String uuid,
    @JsonProperty("fullSchema") BatchSchema fullSchema,
    @JsonProperty("partitionColumnNames") List<String> partitionColumnNames,
    @JsonProperty("icebergOpType") IcebergCommandType icebergOpType,
    @JsonProperty("databaseName") String databaseName,
    @JsonProperty("tableName") String tableName,
    @JsonProperty("dataTableLocation") String dataTableLocation,
    @JsonProperty("versionContext") ResolvedVersionContext version,
    @JsonProperty("partitionSpec") ByteString partitionSpec,
    @JsonProperty("icebergSchema") String icebergSchema) {
      this.tableLocation = tableLocation;
      this.uuid = uuid;
      this.fullSchema = fullSchema;
      this.partitionColumnNames = partitionColumnNames;
      this.icebergOpType = icebergOpType;
      this.databaseName = databaseName;
      this.tableName = tableName;
      this.dataTableLocation = dataTableLocation;
      this.version = version;
      this.partitionSpec = partitionSpec;
      this.icebergSchema = icebergSchema;
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
    this.databaseName = other.databaseName;
    this.partitionSpec = other.partitionSpec;
    this.icebergSchema = other.icebergSchema;
  }

  @Deprecated
  public IcebergTableProps(ByteString partitionSpec){
    this.partitionSpec = partitionSpec;
    this.uuid = null;
  }

  public IcebergTableProps(ByteString partitionSpec, String icebergSchema){
    this.partitionSpec = partitionSpec;
    this.uuid = null;
    this.icebergSchema = icebergSchema;
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

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
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

  public ByteString getPartitionSpec() {
    return partitionSpec;
  }

  public void setPartitionSpec(ByteString partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  public String getIcebergSchema() {
    return icebergSchema;
  }

  public void setIcebergSchema(String icebergSchema) {
    this.icebergSchema = icebergSchema;
  }

  @JsonIgnore
  public PartitionSpec getDeserializedPartitionSpec() {
    return (partitionSpec == null) ? null :
      IcebergSerDe.deserializePartitionSpec(deserializedJsonAsSchema(icebergSchema), partitionSpec.toByteArray());
  }

  @Override
  public String toString() {
    return "IcebergTableProps{" +
      "tableLocation='" + tableLocation + '\'' +
      ", uuid='" + uuid + '\'' +
      ", icebergOpType=" + icebergOpType +
      ", tableName='" + tableName + '\'' +
      ", dataTableLocation='" + dataTableLocation + '\'' +
      ", isMetadataRefresh=" + isMetadataRefresh +
      ", version=" + version +
      '}';
  }

  @JsonIgnore
  public boolean isSchemaSet() {
    return fullSchema != null;
  }
}
