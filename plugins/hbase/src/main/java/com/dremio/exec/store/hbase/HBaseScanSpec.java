/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

public class HBaseScanSpec {

  private final TableName tableName;
  private final byte[] startRow;
  private final byte[] stopRow;
  private final byte[] filter;

  private transient Filter filterParsed;

  public HBaseScanSpec(
      NamespaceKey key,
      byte[] startRow,
      byte[] stopRow,
      byte[] serializedFilter) {
    this.tableName = TableNameGetter.getTableName(key);
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filter = serializedFilter;
  }

  public HBaseScanSpec(
      TableName tableName,
      byte[] startRow,
      byte[] stopRow,
      byte[] serializedFilter) {
    this.tableName = tableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.filter = serializedFilter;
  }

  public HBaseScanSpec(
      TableName tableName,
      byte[] startRow,
      byte[] stopRow,
      Filter filter) {
    this(tableName, startRow, stopRow, HBaseUtils.serializeFilter(filter));
  }

  @JsonIgnore
  public KeyRange getKeyRange() {
    return KeyRange.getRange(startRow, stopRow);
  }

  @JsonIgnore
  public Predicate<PartitionChunkMetadata> getRowKeyPredicate() {
    if( (startRow == null || startRow.length == 0) &&
        (stopRow == null || stopRow.length == 0)
        ){
          return null;
        }

    return new SplitPrune(startRow, stopRow);
  }

  private class SplitPrune implements Predicate<PartitionChunkMetadata> {
    private final KeyRange range;

    private SplitPrune(byte[] start, byte[] stop){
      range = KeyRange.getRange(start, stop);
    }

    @Override
    public boolean apply(PartitionChunkMetadata input) {
      final List<DatasetSplit> splits = ImmutableList.copyOf(input.getDatasetSplits());
      Preconditions.checkArgument(splits.size() == 1, "HBase only supports a single split per partition chunk");
      return KeyRange.fromSplit(splits.get(0)).overlaps(range);
    }
  }

  public TableName getTableName() {
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
