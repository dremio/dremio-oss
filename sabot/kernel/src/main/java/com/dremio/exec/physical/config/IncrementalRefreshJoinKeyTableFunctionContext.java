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

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.protostuff.ByteString;

/**
 * A TableFunctionContext used to pass information needed for IncrementalRefreshJoinKeyTableFunction
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("incremental-refresh-join-key-table")
public class IncrementalRefreshJoinKeyTableFunctionContext extends TableFunctionContext {
  private final ByteString partitionSpec; //target partition spec to apply
  private final String icebergSchema; //Iceberg schema of the data
  private final String metadataLocation; //Location of the Iceberg Metadata for the table

  public IncrementalRefreshJoinKeyTableFunctionContext(@JsonProperty("partitionSpec") final ByteString partitionSpec,
                                                       @JsonProperty("icebergSchema") final String icebergSchema,
                                                       @JsonProperty("schema") final BatchSchema fullSchema,
                                                       @JsonProperty("columns") final List<SchemaPath> columns,
                                                       @JsonProperty("metadataLocation") final String metadataLocation,
                                                       @JsonProperty("pluginId") final StoragePluginId pluginId) {
    super(null, fullSchema, null, null, null, null, pluginId, null, columns, null, null, null, false, false, true, null);
    this.partitionSpec = partitionSpec;
    this.icebergSchema = icebergSchema;
    this.metadataLocation = metadataLocation;
  }

  public ByteString getPartitionSpec() {
    return partitionSpec;
  }

  public String getIcebergSchema() {
    return icebergSchema;
  }

  public String getMetadataLocation(){
    return metadataLocation;
  }
}
