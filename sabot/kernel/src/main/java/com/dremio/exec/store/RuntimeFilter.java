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
package com.dremio.exec.store;

import static com.dremio.sabot.op.scan.ScanOperator.Metric.RUNTIME_COL_FILTER_DROP_COUNT;
import static org.apache.arrow.util.Preconditions.checkArgument;
import static org.apache.arrow.util.Preconditions.checkState;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.sabot.exec.context.OperatorStats;

/**
 * A POJO helper class for the protobuf struct RuntimeFilter
 * The CompositeColumnFilter fields hold the deserialized bloom filter.
 */
public class RuntimeFilter implements AutoCloseable {
  private static Logger logger = LoggerFactory.getLogger(RuntimeFilter.class);
  private CompositeColumnFilter partitionColumnFilter;
  private List<CompositeColumnFilter> nonPartitionColumnFilters;
  private String senderInfo;

  public RuntimeFilter(CompositeColumnFilter partitionColumnFilter, List<CompositeColumnFilter> nonPartitionColumnFilters, String senderInfo) {
    this.partitionColumnFilter = partitionColumnFilter;
    this.nonPartitionColumnFilters = nonPartitionColumnFilters;
    this.senderInfo = senderInfo;
  }

  public CompositeColumnFilter getPartitionColumnFilter() {
    return partitionColumnFilter;
  }

  public List<CompositeColumnFilter> getNonPartitionColumnFilters() {
    return nonPartitionColumnFilters;
  }

  public String getSenderInfo() {
    return senderInfo;
  }

  public static RuntimeFilter getInstance(final ExecProtos.RuntimeFilter protoFilter,
                                          final ArrowBuf msgBuf,
                                          final String senderInfo,
                                          final OperatorStats stats) {
    ExecProtos.CompositeColumnFilter partitionColFilterProto = protoFilter.getPartitionColumnFilter();
    CompositeColumnFilter partitionColFilter = null;
    long nextSliceStart = 0L;
    if (partitionColFilterProto != null && !partitionColFilterProto.getColumnsList().isEmpty()) {
      checkArgument(msgBuf.capacity() >= partitionColFilterProto.getSizeBytes(), "Invalid filter size. " +
              "Buffer capacity is %s, expected filter size %s", msgBuf.capacity(), partitionColFilterProto.getSizeBytes());
      try {
        final BloomFilter bloomFilter = BloomFilter.prepareFrom(msgBuf.slice(nextSliceStart, partitionColFilterProto.getSizeBytes()));
        nextSliceStart += partitionColFilterProto.getSizeBytes();
        checkState(bloomFilter.getNumBitsSet()==partitionColFilterProto.getValueCount(),
                "BloomFilter value count mismatched. Expected %s, Actual %s", partitionColFilterProto.getValueCount(), bloomFilter.getNumBitsSet());
        partitionColFilter = new CompositeColumnFilter.Builder().setProtoFields(protoFilter.getPartitionColumnFilter())
                .setBloomFilter(bloomFilter).build();
        bloomFilter.getDataBuffer().retain();
      } catch (Exception e) {
        stats.addLongStat(RUNTIME_COL_FILTER_DROP_COUNT, 1);
        logger.warn("Error while processing partition column filter from {} : {}", senderInfo, e.getMessage());
      }
    }

    final List<CompositeColumnFilter> nonPartitionColFilters = new ArrayList<>(protoFilter.getNonPartitionColumnFilterCount());
    for (int i =0; i < protoFilter.getNonPartitionColumnFilterCount(); i++) {
      final ExecProtos.CompositeColumnFilter nonPartitionColFilterProto = protoFilter.getNonPartitionColumnFilter(i);
      final String fieldName = nonPartitionColFilterProto.getColumns(0);
      checkArgument(msgBuf.capacity() >= nextSliceStart + nonPartitionColFilterProto.getSizeBytes(),
              "Invalid filter buffer size for non partition col %s.", fieldName);
      try {
        final ValueListFilter valueListFilter = ValueListFilterBuilder
                .fromBuffer(msgBuf.slice(nextSliceStart, nonPartitionColFilterProto.getSizeBytes()));
        nextSliceStart += nonPartitionColFilterProto.getSizeBytes();
        checkState(valueListFilter.getValueCount()==nonPartitionColFilterProto.getValueCount(),
                "ValueListFilter %s count mismatched. Expected %s, found %s", fieldName,
                nonPartitionColFilterProto.getValueCount(), valueListFilter.getValueCount());
        valueListFilter.setFieldName(fieldName);
        final CompositeColumnFilter nonPartitionColFilter = new CompositeColumnFilter.Builder()
                .setProtoFields(nonPartitionColFilterProto).setValueList(valueListFilter).build();
        nonPartitionColFilters.add(nonPartitionColFilter);
        valueListFilter.buf().retain();
      } catch (Exception e) {
        stats.addLongStat(RUNTIME_COL_FILTER_DROP_COUNT, 1);
        logger.warn("Error while processing non-partition column filter on column {}, from {} : {}",
                protoFilter.getNonPartitionColumnFilter(i).getColumns(0), senderInfo, e.getMessage());
      }
    }
    checkState(partitionColFilter != null || !nonPartitionColFilters.isEmpty(), "All filters are dropped.");
    return new RuntimeFilter(partitionColFilter, nonPartitionColFilters, senderInfo);
  }

  public static RuntimeFilter getInstanceWithNewNonPartitionColFiltersList(RuntimeFilter filter) {
    return new RuntimeFilter(filter.getPartitionColumnFilter(), new ArrayList<>(filter.getNonPartitionColumnFilters()),
      filter.getSenderInfo());
  }

  /**
   * Used for identifying duplicate filters.
   *
   * @param that
   * @return
   */
  public boolean isOnSameColumns(final RuntimeFilter that) {
    if (((this.getPartitionColumnFilter() == null) != (that.getPartitionColumnFilter() == null))
            || (this.getNonPartitionColumnFilters().size() != that.getNonPartitionColumnFilters().size())){
      return false;
    }

    final boolean samePartitionColumns = (this.getPartitionColumnFilter() == null) ||
            CollectionUtils.isEqualCollection(this.getPartitionColumnFilter().getColumnsList(), that.getPartitionColumnFilter().getColumnsList());
    final Predicate<CompositeColumnFilter> nonPartitionColFilterHasMatch = f -> that.nonPartitionColumnFilters.stream()
            .anyMatch(t -> f.getColumnsList().equals(t.getColumnsList()));
    final boolean sameNonPartitionColumns = this.getNonPartitionColumnFilters().stream().allMatch(nonPartitionColFilterHasMatch);
    return samePartitionColumns && sameNonPartitionColumns;
  }


  @Override
  public String toString() {
    return "RuntimeFilter{" +
            "partitionColumnFilter=" + partitionColumnFilter +
            ", nonPartitionColumnFilters=" + nonPartitionColumnFilters +
            ", senderInfo='" + senderInfo + '\'' +
            '}';
  }

  @Override
  public void close() throws Exception {
    final List<AutoCloseable> closeables = new ArrayList<>(nonPartitionColumnFilters.size() + 1);
    closeables.addAll(nonPartitionColumnFilters);
    closeables.add(partitionColumnFilter);
    AutoCloseables.close(closeables);
  }
}