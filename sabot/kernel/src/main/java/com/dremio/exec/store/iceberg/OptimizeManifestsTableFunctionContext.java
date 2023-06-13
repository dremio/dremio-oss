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
package com.dremio.exec.store.iceberg;

import java.util.List;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.config.TableFunctionContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.IcebergTableProps;
import com.dremio.service.namespace.dataset.proto.UserDefinedSchemaSettings;
import com.dremio.service.namespace.file.proto.FileConfig;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;

import io.protostuff.ByteString;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("optimize-manifests")
public class OptimizeManifestsTableFunctionContext extends TableFunctionContext {

  private final IcebergTableProps icebergTableProps;

  @JsonIgnore
  public OptimizeManifestsTableFunctionContext(TableMetadata tableMetadata,
                                               BatchSchema outputSchema,
                                               IcebergTableProps icebergTableProps) {
    this(
      tableMetadata.getFormatSettings(),
      outputSchema,
      outputSchema.getFields().stream()
        .map(f -> SchemaPath.getSimplePath(f.getName()))
        .collect(ImmutableList.toImmutableList()),
      ImmutableList.of(tableMetadata.getName().getPathComponents()),
      tableMetadata.getStoragePluginId(),
      tableMetadata.getReadDefinition().getPartitionColumnsList(),
      tableMetadata.getReadDefinition().getExtendedProperty(),
      tableMetadata.getDatasetConfig().getPhysicalDataset().getInternalSchemaSettings(),
      icebergTableProps
    );
  }

  @JsonCreator
  public OptimizeManifestsTableFunctionContext(@JsonProperty("formatSettings") FileConfig formatSettings,
                                               @JsonProperty("schema") BatchSchema outputSchema,
                                               @JsonProperty("columns") List<SchemaPath> columns,
                                               @JsonProperty("referencedTables") List<List<String>> tablePath,
                                               @JsonProperty("pluginId") StoragePluginId pluginId,
                                               @JsonProperty("partitionColumns") List<String> partitionColumns,
                                               @JsonProperty("extendedProperty") ByteString extendedProperty,
                                               @JsonProperty("userDefinedSchemaSettings") UserDefinedSchemaSettings userDefinedSchemaSettings,
                                               @JsonProperty("icebergTableProps") IcebergTableProps icebergTableProps) {
    super(
      formatSettings,
      outputSchema,
      outputSchema,
      tablePath,
      null,
      pluginId,
      null,
      columns,
      partitionColumns,
      null,
      extendedProperty,
      false, false, false,
      userDefinedSchemaSettings
    );
    this.icebergTableProps = icebergTableProps;
  }

  public IcebergTableProps getIcebergTableProps() {
    return icebergTableProps;
  }
}
