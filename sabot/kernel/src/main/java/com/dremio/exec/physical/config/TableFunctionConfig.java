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

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Table function config
 */
@JsonTypeName("table-function-config")
public class TableFunctionConfig {
  public enum FunctionType {
    UNKNOWN,
    METADATA_REFRESH_MANIFEST_SCAN,
    SPLIT_GEN_MANIFEST_SCAN,
    DATA_FILE_SCAN,
    SPLIT_GENERATION,
    FOOTER_READER,
    SCHEMA_AGG,
    SPLIT_ASSIGNMENT
  }
  private final FunctionType type;
  private final TableFunctionContext functionContext;
  private final boolean fillBatch;
  public TableFunctionConfig(
    @JsonProperty("type") FunctionType type,
    @JsonProperty("fillBatch") boolean fillBatch,
    @JsonProperty("functioncontext") TableFunctionContext functionContext
  ) {
    this.type = type;
    this.functionContext = functionContext;
    this.fillBatch = fillBatch;
  }

  public FunctionType getType() {
    return type;
  }

  public TableFunctionContext getFunctionContext() {
    return functionContext;
  }

  public boolean getFillBatch() {
    return fillBatch;
  }

  @JsonIgnore
  public BatchSchema getOutputSchema() {
    return functionContext.getFullSchema().maskAndReorder(
      functionContext.getColumns());
  }

  @JsonIgnore
  public BatchSchema getTableSchema() {
    return functionContext.getTableSchema();
  }

}
