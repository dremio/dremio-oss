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
import com.dremio.exec.store.parquet.ParquetScanRowGroupFilter;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.protostuff.ByteString;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("split-production")
public class SplitProducerTableFunctionContext extends TableFunctionContext {

  /**
   * If true exactly one (block) split will be generated for the whole file, if false the file may
   * be broken up into several splits
   */
  private final boolean isOneSplitPerFile;

  public SplitProducerTableFunctionContext(
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
      @JsonProperty("isOneSplitPerFile") boolean isOneSplitPerFile) {
    super(
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
        userDefinedSchemaSettings);
    this.isOneSplitPerFile = isOneSplitPerFile;
  }

  @JsonProperty("isOneSplitPerFile")
  public boolean isOneSplitPerFile() {
    return isOneSplitPerFile;
  }
}
