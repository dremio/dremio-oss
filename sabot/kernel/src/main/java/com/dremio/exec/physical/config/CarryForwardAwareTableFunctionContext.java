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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.physical.visitor.GlobalDictionaryFieldInfo;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.util.SchemaPathMapDeserializer;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import io.protostuff.ByteString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("carry-forward-enabled-context")
public class CarryForwardAwareTableFunctionContext extends TableFunctionContext {
  private final boolean isCarryForwardEnabled;
  private final Map<SchemaPath, SchemaPath> inputColMap;
  private final String constValCol;
  private final String constVal;

  @JsonCreator
  public CarryForwardAwareTableFunctionContext(@JsonProperty("formatSettings") FileConfig formatSettings,
                                               @JsonProperty("schema") BatchSchema fullSchema,
                                               @JsonProperty("tableschema") BatchSchema tableSchema,
                                               @JsonProperty("referencedTables") List<List<String>> tablePath,
                                               @JsonProperty("scanFilter") ScanFilter scanFilter,
                                               @JsonProperty("pluginId") StoragePluginId pluginId,
                                               @JsonProperty("internalTablePluginId") StoragePluginId internalTablePluginId,
                                               @JsonProperty("columns") List<SchemaPath> columns,
                                               @JsonProperty("partitionColumns") List<String> partitionColumns,
                                               @JsonProperty("globalDictionaryEncodedColumns") List<GlobalDictionaryFieldInfo> globalDictionaryEncodedColumns,
                                               @JsonProperty("extendedProperty") ByteString extendedProperty,
                                               @JsonProperty("arrowCachingEnabled") boolean arrowCachingEnabled,
                                               @JsonProperty("convertedIcebergDataset") boolean isConvertedIcebergDataset,
                                               @JsonProperty("icebergMetadata") boolean isIcebergMetadata,
                                               @JsonProperty("userDefinedSchemaSettings") UserDefinedSchemaSettings userDefinedSchemaSettings,
                                               @JsonProperty("carryForwardEnabled") boolean isCarryForwardEnabled,
                                               @JsonProperty("inputColMap") @JsonDeserialize(using = SchemaPathMapDeserializer.class) Map<SchemaPath, SchemaPath> inputColMap,
                                               @JsonProperty("constValueCol") String constValCol,
                                               @JsonProperty("constValue") String constVal) {
    super(formatSettings, fullSchema, tableSchema, tablePath, scanFilter, null, pluginId, internalTablePluginId, columns, partitionColumns, globalDictionaryEncodedColumns,
      extendedProperty, arrowCachingEnabled, isConvertedIcebergDataset, isIcebergMetadata, userDefinedSchemaSettings);

    this.isCarryForwardEnabled = isCarryForwardEnabled;
    this.inputColMap = inputColMap;
    this.constValCol = constValCol;
    this.constVal = constVal;
  }

  public CarryForwardAwareTableFunctionContext(BatchSchema outputSchema, StoragePluginId storagePluginId,
                                               boolean isCarryForwardEnabled, Map<SchemaPath, SchemaPath> inputColMap,
                                               String constValCol, String constVal) {
    this(
      null,
      outputSchema,
      null,
      null,
      null,
      storagePluginId,
      null,
      outputSchema.getFields().stream().map(f -> SchemaPath.getSimplePath(f.getName())).collect(Collectors.toList()),
      null,
      null,
      null,
      false,
      false,
      false,
      null,
      isCarryForwardEnabled,
      inputColMap,
      constValCol,
      constVal);
  }

  public boolean isCarryForwardEnabled() {
    return isCarryForwardEnabled;
  }

  public Map<SchemaPath, SchemaPath> getInputColMap() {
    return inputColMap;
  }

  public String getConstValCol() {
    return constValCol;
  }

  public String getConstVal() {
    return constVal;
  }
}
