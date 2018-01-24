/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store.hbase;

import org.apache.hadoop.hbase.filter.Filter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Scan specification for HBase Region range.
 */
public class HBaseSubScanSpec {

  private final String namespace;
  private final String tableName;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final byte[] serializedFilter;

  @JsonCreator
  public HBaseSubScanSpec(
      @JsonProperty("namespace") String namespace,
      @JsonProperty("tableName") String tableName,
      @JsonProperty("startRow") byte[] startRow,
      @JsonProperty("stopRow") byte[] stopRow,
      @JsonProperty("serializedFilter") byte[] serializedFilter) {
    this.namespace = namespace;
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.serializedFilter = serializedFilter;
  }

  @JsonIgnore
  public Filter asScanFilter() {
    if(serializedFilter == null) {
      return null;
    }

    return HBaseUtils.deserializeFilter(serializedFilter);
  }

  public String getNamespace() {
    return namespace;
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public byte[] getSerializedFilter() {
    return serializedFilter;
  }


}