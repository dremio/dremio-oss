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
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * TableFunctionContext specific for IcebergMergeOnReadRowSplitterTableFunction and its respective
 * Prel.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("merge-on-read-row-splitter")
public class MergeOnReadRowSplitterTableFunctionContext extends TableFunctionContext {

  private final int insertColumnCount;
  private final Map<String, Integer> updateColumnsWithIndex;
  private final Set<String> outdatedTargetColumnNames;

  public MergeOnReadRowSplitterTableFunctionContext(
      @JsonProperty("schema") BatchSchema fullSchema,
      @JsonProperty("tableSchema") BatchSchema tableSchema,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("insertColumnCount") int insertColumnCount,
      @JsonProperty("updateColumnsWithIndex") Map<String, Integer> updateColumnsWithIndex,
      @JsonProperty("outdatedTargetColumnNames") Set<String> outdatedTargetColumnNames,
      @JsonProperty("partitionColumns") List<String> partitionColumns) {
    super(
        null,
        fullSchema,
        tableSchema,
        null,
        null,
        null,
        null,
        null,
        columns,
        partitionColumns,
        null,
        false,
        false,
        false,
        null);
    this.insertColumnCount = insertColumnCount;
    this.updateColumnsWithIndex = updateColumnsWithIndex;
    this.outdatedTargetColumnNames = outdatedTargetColumnNames;
  }

  public int getInsertColumnCount() {
    return insertColumnCount;
  }

  public Map<String, Integer> getUpdateColumnsWithIndex() {
    return updateColumnsWithIndex;
  }

  public Set<String> getOutdatedTargetColumnNames() {
    return outdatedTargetColumnNames;
  }
}
