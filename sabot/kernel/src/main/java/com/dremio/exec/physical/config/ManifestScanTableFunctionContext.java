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
import com.dremio.exec.store.iceberg.ManifestContentType;
import com.dremio.exec.util.SchemaPathMapDeserializer;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.protostuff.ByteString;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("manifest-scan")
public class ManifestScanTableFunctionContext extends CarryForwardAwareTableFunctionContext {

  private final ByteString partitionSpecMap;
  private final ByteString jsonPartitionSpecMap;
  private String icebergSchema;
  private final ManifestContentType manifestContentType;
  private final ManifestScanFilters manifestScanFilters;
  private final boolean
      publishPartitionInfo; // Process and output partitions only if the flag is turned ON, skipped
  // otherwise.

  // Only has effect if publishPartitionInfo is on.
  // If enabled IcebergPartitionValue is populated in the PartitionProtobuf.NormalizedPartitionInfo
  private final boolean includeIcebergPartitionInfo;

  public ManifestScanTableFunctionContext(
      @JsonProperty("partitionSpecMap") ByteString partitionSpecMap,
      @JsonProperty("jsonPartitionSpecMap") ByteString jsonPartitionSpecMap,
      @JsonProperty("icebergSchema") String icebergSchema,
      @JsonProperty("formatSettings") FileConfig formatSettings,
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("tableschema") BatchSchema tableSchema,
      @JsonProperty("referencedTables") List<List<String>> tablePath,
      @JsonProperty("scanFilter") ScanFilter scanFilter,
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
      @JsonProperty("manifestContentType") final ManifestContentType manifestContentType,
      @JsonProperty("metadataFilters") ManifestScanFilters manifestScanFilters,
      @JsonProperty("carryForwardEnabled") boolean isCarryForwardEnabled,
      @JsonProperty("inputColMap") @JsonDeserialize(using = SchemaPathMapDeserializer.class)
          Map<SchemaPath, SchemaPath> inputColMap,
      @JsonProperty("constValueCol") String constValCol,
      @JsonProperty("constValue") String constVal,
      @JsonProperty("publishPartitionInfo") boolean publishPartitionInfo,
      @JsonProperty("includeIcebergPartitionInfo") boolean includeIcebergPartitionInfo,
      @JsonProperty("schemeVariate") String schemeVariate) {
    super(
        formatSettings,
        fullSchema,
        tableSchema,
        tablePath,
        scanFilter,
        pluginId,
        internalTablePluginId,
        columns,
        partitionColumns,
        extendedProperty,
        arrowCachingEnabled,
        isConvertedIcebergDataset,
        isIcebergMetadata,
        userDefinedSchemaSettings,
        isCarryForwardEnabled,
        inputColMap,
        constValCol,
        constVal,
        schemeVariate);
    this.partitionSpecMap = partitionSpecMap;
    this.icebergSchema = icebergSchema;
    this.jsonPartitionSpecMap = jsonPartitionSpecMap;
    this.manifestScanFilters = manifestScanFilters;
    this.manifestContentType = manifestContentType;
    this.publishPartitionInfo = publishPartitionInfo;
    this.includeIcebergPartitionInfo = includeIcebergPartitionInfo;
  }

  public ByteString getPartitionSpecMap() {
    return partitionSpecMap;
  }

  public ByteString getJsonPartitionSpecMap() {
    return jsonPartitionSpecMap;
  }

  public String getIcebergSchema() {
    return icebergSchema;
  }

  public ManifestScanFilters getManifestScanFilters() {
    return manifestScanFilters;
  }

  public ManifestContentType getManifestContentType() {
    return manifestContentType;
  }

  public boolean getIncludeIcebergPartitionInfo() {
    return includeIcebergPartitionInfo;
  }

  public boolean isPublishPartitionInfo() {
    return publishPartitionInfo;
  }
}
