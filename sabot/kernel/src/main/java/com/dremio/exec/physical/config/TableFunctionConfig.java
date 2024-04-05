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
import com.google.common.base.Preconditions;

/** Table function config */
@JsonTypeName("table-function-config")
public class TableFunctionConfig {
  public enum FunctionType {
    UNKNOWN,
    METADATA_MANIFEST_FILE_SCAN,
    SPLIT_GEN_MANIFEST_SCAN,
    DATA_FILE_SCAN,
    EASY_DATA_FILE_SCAN,
    SPLIT_GENERATION,
    EASY_SPLIT_GENERATION,
    DIR_LISTING_SPLIT_GENERATION,
    FOOTER_READER,
    SCHEMA_AGG,
    SPLIT_ASSIGNMENT,
    BOOST_TABLE_FUNCTION,
    ICEBERG_PARTITION_TRANSFORM,
    DELETED_FILES_METADATA,
    ICEBERG_SPLIT_GEN,
    ICEBERG_MANIFEST_SCAN,
    ICEBERG_DELETE_FILE_AGG,
    ICEBERG_DML_MERGE_DUPLICATE_CHECK,
    ICEBERG_OPTIMIZE_MANIFESTS,
    ICEBERG_ORPHAN_FILE_DELETE,
    ICEBERG_MANIFEST_LIST_SCAN,
    ICEBERG_PARTITION_STATS_SCAN,
    DIR_LISTING,
    ICEBERG_TABLE_LOCATION_FINDER,
    ICEBERG_SNAPSHOTS_SCAN,
    ICEBERG_INCREMENTAL_REFRESH_JOIN_KEY,
    DELTALAKE_HISTORY_SCAN
  }

  private final FunctionType type;
  private final TableFunctionContext functionContext;
  private final boolean fillBatch;
  private long minWidth = -1;
  private long maxWidth = -1;

  public TableFunctionConfig(
      @JsonProperty("type") FunctionType type,
      @JsonProperty("fillBatch") boolean fillBatch,
      @JsonProperty("functioncontext") TableFunctionContext functionContext) {
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

  public <T extends TableFunctionContext> T getFunctionContext(Class<T> clazz) {
    Preconditions.checkArgument(clazz.isInstance(functionContext));
    return clazz.cast(functionContext);
  }

  public boolean getFillBatch() {
    return fillBatch;
  }

  @JsonIgnore
  public BatchSchema getOutputSchema() {
    return functionContext.getFullSchema().maskAndReorder(functionContext.getColumns());
  }

  @JsonIgnore
  public BatchSchema getTableSchema() {
    return functionContext.getTableSchema();
  }

  public long getMinWidth() {
    return minWidth;
  }

  public long getMaxWidth() {
    return maxWidth;
  }

  public void setMinWidth(long minWidth) {
    this.minWidth = minWidth;
  }

  public void setMaxWidth(long maxWidth) {
    this.maxWidth = maxWidth;
  }
}
