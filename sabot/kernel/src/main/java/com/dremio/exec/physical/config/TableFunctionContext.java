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
package com.dremio.exec.physical.config;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.iceberg.OptimizeManifestsTableFunctionContext;
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.protostuff.ByteString;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** Table function context Extend this class to add props related to specific table function */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
  @JsonSubTypes.Type(value = FooterReaderTableFunctionContext.class, name = "footer-reader"),
  @JsonSubTypes.Type(value = BoostTableFunctionContext.class, name = "boost"),
  @JsonSubTypes.Type(value = ManifestScanTableFunctionContext.class, name = "manifest-scan"),
  @JsonSubTypes.Type(
      value = PartitionTransformTableFunctionContext.class,
      name = "partition-transform-table"),
  @JsonSubTypes.Type(value = EasyScanTableFunctionContext.class, name = "easy-scan-table-function"),
  @JsonSubTypes.Type(
      value = DeletedFilesMetadataTableFunctionContext.class,
      name = "deleted-files-metadata-table-function"),
  @JsonSubTypes.Type(
      value = OptimizeManifestsTableFunctionContext.class,
      name = "optimize-manifests"),
  @JsonSubTypes.Type(value = DirListingTableFunctionContext.class, name = "dir-listing"),
  @JsonSubTypes.Type(
      value = CarryForwardAwareTableFunctionContext.class,
      name = "carry-forward-enabled-context"),
  @JsonSubTypes.Type(value = IcebergLocationFinderFunctionContext.class, name = "location-finder"),
  @JsonSubTypes.Type(
      value = IncrementalRefreshJoinKeyTableFunctionContext.class,
      name = "incremental-refresh-join-key-table"),
  @JsonSubTypes.Type(
      value = OrphanFileDeleteTableFunctionContext.class,
      name = "orphan-file-delete"),
  @JsonSubTypes.Type(value = SplitProducerTableFunctionContext.class, name = "split-production"),
  @JsonSubTypes.Type(
      value = MergeOnReadRowSplitterTableFunctionContext.class,
      name = "merge-on-read-row-splitter")
})
public class TableFunctionContext {
  private final List<SchemaPath> columns;
  private final ScanFilter scanFilter;
  private final StoragePluginId pluginId;
  private final StoragePluginId internalTablePluginId;
  private final FileConfig formatSettings;
  private final List<String> partitionColumns;
  private final List<List<String>> tablePath;
  private final ByteString extendedProperty;
  private final boolean arrowCachingEnabled;
  private final BatchSchema fullSchema;
  private final BatchSchema tableSchema;
  private final Collection<List<String>> referencedTables;
  private final boolean isConvertedIcebergDataset;
  private final boolean isIcebergMetadata;
  private final UserDefinedSchemaSettings userDefinedSchemaSettings;
  private final Map<String, Integer> colIdMap;
  private final ParquetScanRowGroupFilter rowGroupFilter;

  public TableFunctionContext(
      @JsonProperty("formatSettings") FileConfig formatSettings,
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("tableschema") BatchSchema tableSchema,
      @JsonProperty("referencedTables") List<List<String>> tablePath,
      @JsonProperty("scanFilter") ScanFilter scanFilter,
      @JsonProperty("rowGroupFilter") ParquetScanRowGroupFilter rowGroupFilter,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("internalTablePluginId") StoragePluginId internalTablePluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("partitionColumns") List<String> partitionColumns,
      @JsonProperty("extendedProperty") ByteString extendedProperty,
      @JsonProperty("arrowCachingEnabled") boolean arrowCachingEnabled,
      @JsonProperty("convertedIcebergDataset") boolean isConvertedIcebergDataset,
      @JsonProperty("icebergMetadata") boolean isIcebergMetadata,
      @JsonProperty("userDefinedSchemaSettings")
          UserDefinedSchemaSettings userDefinedSchemaSettings,
      @JsonProperty("colIdMap") Map<String, Integer> colIdMap) {
    this.fullSchema = fullSchema;
    this.tableSchema = tableSchema;
    this.referencedTables = tablePath;
    this.columns = columns;
    this.formatSettings = formatSettings;
    this.tablePath = tablePath;
    this.scanFilter = scanFilter;
    this.rowGroupFilter = rowGroupFilter;
    this.pluginId = pluginId;
    this.internalTablePluginId = internalTablePluginId;
    this.partitionColumns = partitionColumns;
    this.extendedProperty = extendedProperty;
    this.arrowCachingEnabled = arrowCachingEnabled;
    this.isConvertedIcebergDataset = isConvertedIcebergDataset;
    this.isIcebergMetadata = isIcebergMetadata;
    this.userDefinedSchemaSettings = userDefinedSchemaSettings;
    this.colIdMap = colIdMap;
  }

  public TableFunctionContext(
      FileConfig formatSettings,
      BatchSchema fullSchema,
      BatchSchema tableSchema,
      List<List<String>> tablePath,
      ScanFilter scanFilter,
      ParquetScanRowGroupFilter rowGroupFilter,
      StoragePluginId pluginId,
      StoragePluginId internalTablePluginId,
      List<SchemaPath> columns,
      List<String> partitionColumns,
      ByteString extendedProperty,
      boolean arrowCachingEnabled,
      boolean isConvertedIcebergDataset,
      boolean isIcebergMetadata,
      UserDefinedSchemaSettings userDefinedSchemaSettings) {
    this(
        formatSettings,
        fullSchema,
        tableSchema,
        tablePath,
        scanFilter,
        rowGroupFilter,
        pluginId,
        internalTablePluginId,
        columns,
        partitionColumns,
        extendedProperty,
        arrowCachingEnabled,
        isConvertedIcebergDataset,
        isIcebergMetadata,
        userDefinedSchemaSettings,
        null);
  }

  public TableFunctionContext(
      BatchSchema batchSchema, List<SchemaPath> columns, boolean isIcebergMetadata) {
    this(
        null,
        batchSchema,
        null,
        null,
        null,
        null,
        null,
        null,
        columns,
        null,
        null,
        false,
        false,
        isIcebergMetadata,
        null,
        null);
  }

  public TableFunctionContext(BatchSchema batchSchema, List<SchemaPath> columns) {
    this(batchSchema, columns, false);
  }

  public FileConfig getFormatSettings() {
    return formatSettings;
  }

  public List<SchemaPath> getColumns() {
    return columns;
  }

  public List<String> getPartitionColumns() {
    return partitionColumns;
  }

  public List<List<String>> getTablePath() {
    return tablePath;
  }

  public ByteString getExtendedProperty() {
    return extendedProperty;
  }

  public ScanFilter getScanFilter() {
    return scanFilter;
  }

  public ParquetScanRowGroupFilter getRowGroupFilter() {
    return rowGroupFilter;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public StoragePluginId getInternalTablePluginId() {
    return internalTablePluginId;
  }

  public boolean isArrowCachingEnabled() {
    return arrowCachingEnabled;
  }

  public boolean isConvertedIcebergDataset() {
    return isConvertedIcebergDataset;
  }

  public boolean isIcebergMetadata() {
    return isIcebergMetadata;
  }

  @JsonProperty("fullSchema")
  public BatchSchema getFullSchema() {
    return fullSchema;
  }

  @JsonProperty("tableSchema")
  public BatchSchema getTableSchema() {
    return tableSchema;
  }

  public Collection<List<String>> getReferencedTables() {
    return referencedTables;
  }

  public UserDefinedSchemaSettings getUserDefinedSchemaSettings() {
    return userDefinedSchemaSettings;
  }

  public Map<String, Integer> getColIdMap() {
    return colIdMap;
  }
}
