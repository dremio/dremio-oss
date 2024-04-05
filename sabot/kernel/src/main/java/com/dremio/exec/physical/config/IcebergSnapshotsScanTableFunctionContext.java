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
import com.dremio.exec.store.iceberg.SnapshotsScanOptions;
import com.dremio.exec.util.SchemaPathMapDeserializer;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("snapshots-scan")
public class IcebergSnapshotsScanTableFunctionContext
    extends CarryForwardAwareTableFunctionContext {
  private final SnapshotsScanOptions snapshotsScanOptions;

  public IcebergSnapshotsScanTableFunctionContext(
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("snapshotsScanOptions") SnapshotsScanOptions snapshotsScanOptions,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("projectedCols") List<SchemaPath> projectedCols,
      @JsonProperty("carryForwardEnabled") boolean isCarryForwardEnabled,
      @JsonProperty("inputColMap") @JsonDeserialize(using = SchemaPathMapDeserializer.class)
          Map<SchemaPath, SchemaPath> inputColMap,
      @JsonProperty("constValueCol") String constValCol,
      @JsonProperty("constValue") String constVal) {
    super(
        null,
        schema,
        schema,
        null,
        null,
        pluginId,
        null,
        projectedCols,
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
    this.snapshotsScanOptions = snapshotsScanOptions;
  }

  public SnapshotsScanOptions getSnapshotsScanOptions() {
    return snapshotsScanOptions;
  }
}
