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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("orphan-file-delete")
public class OrphanFileDeleteTableFunctionContext extends TableFunctionContext {
  private final String tableLocation; // The location should include file scheme info

  public OrphanFileDeleteTableFunctionContext(
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("tableLocation") String tableLocation) {
    super(
        null,
        fullSchema,
        null,
        null,
        null,
        null,
        pluginId,
        null,
        columns,
        null,
        null,
        false,
        false,
        false,
        null);
    this.tableLocation = tableLocation;
  }

  public String getTableLocation() {
    return tableLocation;
  }
}
