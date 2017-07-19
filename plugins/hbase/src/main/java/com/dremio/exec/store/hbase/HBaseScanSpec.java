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


import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

public class HBaseScanSpec {

  final protected String tableName;
  final protected byte[] startRow;
  final protected byte[] stopRow;
  protected byte[] filter;

  protected Filter filterParsed;

  @JsonCreator
  public HBaseScanSpec(@JsonProperty("tableName") String tableName,
                       @JsonProperty("startRow") byte[] startRow,
                       @JsonProperty("stopRow") byte[] stopRow,
                       @JsonProperty("serializedFilter") byte[] serializedFilter,
                       @JsonProperty("filterString") String filterString) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    if (filterString != null) {
      this.filter = HBaseUtils.serializeFilter(HBaseUtils.parseFilterString(filterString));
    } else {
      this.filter = serializedFilter;
    }
    if (serializedFilter != null && filterString != null) {
      throw new IllegalArgumentException("The parameters 'serializedFilter' or 'filterString' cannot be specified at the same time.");
    }
  }

  public HBaseScanSpec(String tableName, byte[] startRow, byte[] stopRow, Filter filter) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    if (filter != null) {
      this.filter = HBaseUtils.serializeFilter(filter);
    } else {
      this.filter = null;
    }
  }

  public HBaseScanSpec(String tableName) {
    this(tableName, null, null, null);
  }

  public String getTableName() {
    return tableName;
  }

  public byte[] getStartRow() {
    return startRow == null ? HConstants.EMPTY_START_ROW : startRow;
  }

  public byte[] getStopRow() {
    return stopRow == null ? HConstants.EMPTY_START_ROW : stopRow;
  }

  @JsonIgnore
  public Filter getFilter() {
    if (filterParsed == null) {
      synchronized(this) {
        if (filterParsed == null) {
          filterParsed = HBaseUtils.deserializeFilter(this.filter);
        }
      }
    }
    return filterParsed;
  }

  public byte[] getSerializedFilter() {
    return (this.filter != null) ? this.filter : null;
  }

  @Override
  public String toString() {
    Filter filterToString = getFilter();
    return "HBaseScanSpec [tableName=" + tableName
        + ", startRow=" + (startRow == null ? null : Bytes.toStringBinary(startRow))
        + ", stopRow=" + (stopRow == null ? null : Bytes.toStringBinary(stopRow))
        + ", filter=" + (filter == null ? null : filterToString.toString())
        + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HBaseScanSpec that = (HBaseScanSpec) o;
    return Objects.equal(tableName, that.tableName) &&
        Objects.equal(startRow, that.startRow) &&
        Objects.equal(stopRow, that.stopRow) &&
        Objects.equal(filter, that.filter);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(tableName, startRow, stopRow, filter);
  }
}
