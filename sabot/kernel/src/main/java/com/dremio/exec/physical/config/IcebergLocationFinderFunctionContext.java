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
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("location-finder")
public class IcebergLocationFinderFunctionContext extends TableFunctionContext {

  private final Map<String, String> tablePropertiesSkipCriteria;
  private final boolean continueOnError;

  public IcebergLocationFinderFunctionContext(
      @JsonProperty("pluginId") StoragePluginId storagePluginId,
      @JsonProperty("schema") BatchSchema outputSchema,
      @JsonProperty("projectedCols") List<SchemaPath> projectedCols,
      @JsonProperty("tablePropertiesSkipCriteria") Map<String, String> tablePropertiesSkipCriteria,
      @JsonProperty("continueOnError") boolean continueOnError) {
    super(
        null,
        outputSchema,
        null,
        null,
        null,
        null,
        storagePluginId,
        null,
        projectedCols,
        null,
        null,
        false,
        false,
        false,
        null);
    this.tablePropertiesSkipCriteria = tablePropertiesSkipCriteria;
    this.continueOnError = continueOnError;
  }

  public Map<String, String> getTablePropertiesSkipCriteria() {
    return tablePropertiesSkipCriteria;
  }

  @JsonProperty("continueOnError")
  public boolean continueOnError() {
    return continueOnError;
  }
}
