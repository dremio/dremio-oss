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
import static com.dremio.exec.store.iceberg.IcebergSerDe.serializedSchemaAsJson;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.TableMetadata;

import com.dremio.catalog.model.ResolvedVersionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.iceberg.IcebergSerDe;
import com.dremio.exec.store.iceberg.SchemaConverter;
import com.dremio.exec.store.iceberg.model.IcebergCommandType;
import com.dremio.service.namespace.file.proto.FileType;
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
  private String sortOrder;

  // TODO: Separate action specific props from "Table Metadata" related props
  private boolean detectSchema;
  private boolean isMetadataRefresh;
  private IcebergCommandType icebergOpType;
  private Long metadataExpireAfterMs;
  private Map<String, String> tableProperties;
  private FileType fileType;

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
    @JsonProperty("icebergSchema") String icebergSchema,
    @JsonProperty("metadataExpireAfterMs") Long metadataExpireAfterMs,
    @JsonProperty("sortOrder") String sortOrder,
    @JsonProperty("tableProperties") Map<String, String> tableProperties,
    @JsonProperty("fileType") FileType fileType) {
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
      this.metadataExpireAfterMs = metadataExpireAfterMs;
      this.sortOrder = sortOrder;
      this.tableProperties = tableProperties;
      this.fileType = fileType;
  }

  /**
   * Create properties instance from TableMetadata. Doesn't account for table and DB name, since they are stored only
   * in the Catalog (a layer above)
   */
  public static IcebergTableProps createInstance(IcebergCommandType commandType, String tableName, String dbName,
                                                 TableMetadata tableMetadata, ResolvedVersionContext version) {
    BatchSchema schema = SchemaConverter.getBuilder().build().fromIceberg(tableMetadata.schema());
    String jsonSchema = serializedSchemaAsJson(SchemaConverter.getBuilder().build().toIcebergSchema(schema));
    return new IcebergTableProps(tableMetadata.location(), tableMetadata.uuid(), schema,
      tableMetadata.spec().fields().stream().map(f -> f.name()).collect(Collectors.toList()),
      commandType, dbName, tableName, null, version,
      ByteString.copyFrom(IcebergSerDe.serializePartitionSpec(tableMetadata.spec())), jsonSchema, null, null, tableMetadata.properties(), null);
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
    this.metadataExpireAfterMs = other.metadataExpireAfterMs;
    this.sortOrder = other.sortOrder;
    this.tableProperties = other.tableProperties;
    this.fileType = other.fileType;
  }

  @Deprecated
  public IcebergTableProps(ByteString partitionSpec){
    this.partitionSpec = partitionSpec;
    this.uuid = null;
  }

  public IcebergTableProps(ByteString partitionSpec, String icebergSchema, Map<String, String> tableProperties){
    this.partitionSpec = partitionSpec;
    this.uuid = null;
    this.icebergSchema = icebergSchema;
    this.tableProperties = tableProperties;
  }

  public IcebergTableProps(ByteString partitionSpec, String icebergSchema, String sortOrder, Map<String, String> tableProperties) {
    this.partitionSpec = partitionSpec;
    this.uuid = null;
    this.icebergSchema = icebergSchema;
    this.sortOrder = sortOrder;
    this.tableProperties = tableProperties;
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

  public String getSortOrder() {
    return sortOrder;
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

  public Long getMetadataExpireAfterMs() {
    return metadataExpireAfterMs;
  }

  public void setMetadataExpireAfterMs(Long metadataExpireAfterMs) {
    this.metadataExpireAfterMs = metadataExpireAfterMs;
  }

  public Map<String, String> getTableProperties() {
    return tableProperties != null ? tableProperties : Collections.emptyMap();
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

  public FileType getFileType(){
    return fileType;
  }

  public void setFileType(FileType fileType) {
    this.fileType = fileType;
  }
}
