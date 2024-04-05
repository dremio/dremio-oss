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
package com.dremio.exec.store.mfunctions;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractSubScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.tablefunctions.MetadataFunctionsMacro;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import java.util.List;

@JsonTypeName("metadata-functions-sub-scan")
public final class MetadataFunctionsSubScan extends AbstractSubScan {

  static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(MetadataFunctionsSubScan.class);

  private final StoragePluginId pluginId;
  private final BatchSchema schema;
  private final List<SchemaPath> columns;
  private final MetadataFunctionsMacro.MacroName mFunction;
  private final String metadataLocation;

  @JsonCreator
  public MetadataFunctionsSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("fullSchema") BatchSchema schema,
      @JsonProperty("mFunction") MetadataFunctionsMacro.MacroName mFunction,
      @JsonProperty("tableSchemaPath") List<String> tablePath,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("metadataLocation") String metadataLocation) {
    super(props, schema, tablePath == null ? null : ImmutableList.of(tablePath));
    this.metadataLocation = metadataLocation;
    this.schema = schema;
    this.pluginId = pluginId;
    this.columns = columns;
    this.mFunction = mFunction;
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public BatchSchema getSchema() {
    return schema;
  }

  public String getMetadataLocation() {
    return metadataLocation;
  }

  public MetadataFunctionsMacro.MacroName getmFunction() {
    return mFunction;
  }

  @Override
  public int getOperatorType() {
    return UserBitShared.CoreOperatorType.ICEBERG_METADATA_FUNCTIONS_READER.getNumber();
  }

  @Override
  public List<SchemaPath> getColumns() {
    return columns;
  }
}
