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

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("dir-listing")
public class DirListingTableFunctionContext extends TableFunctionContext{
  private final boolean allowRecursiveListing;

  // Extends URL with a version query parameter, based on the last modification time of the table
  private final boolean hasVersion;

  public DirListingTableFunctionContext(
    @JsonProperty("schema") BatchSchema fullSchema,
    @JsonProperty("tableschema") BatchSchema tableSchema,
    @JsonProperty("pluginId") StoragePluginId pluginId,
    @JsonProperty("columns") List<SchemaPath> columns,
    @JsonProperty("allowRecursiveListing") boolean allowRecursiveListing,
    @JsonProperty("hasVersion") boolean hasVersion) {
    super(null, fullSchema, tableSchema, null, null, null, pluginId, null, columns, null, null,
      null, false, false, false, null);
    this.allowRecursiveListing = allowRecursiveListing;
    this.hasVersion = hasVersion;
  }

  @JsonProperty("allowRecursiveListing")
  public boolean allowRecursiveListing() {
    return allowRecursiveListing;
  }

  @JsonProperty("hasVersion")
  public boolean hasVersion() {
    return hasVersion;
  }
}
