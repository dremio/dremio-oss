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
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.util.BloomFilter;
import com.dremio.exec.util.ValueListFilter;
import com.dremio.exec.util.ValueListFilterBuilder;
import com.dremio.exec.util.ValueListWithBloomFilter;
import com.dremio.options.OptionManager;
import com.dremio.sabot.exec.context.OperatorStats;


/**
 * A POJO helper class for the protobuf struct RuntimeFilter
 * The CompositeColumnFilter fields hold the deserialized bloom filter.
 */
public class RuntimeFilter implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(RuntimeFilter.class);
  private CompositeColumnFilter partitionColumnFilter;
  private List<CompositeColumnFilter> nonPartitionColumnFilters;
  private String senderInfo;
  private List<UserBitShared.RunTimeFilterDetailsInfoInScan> filterDetails;

  public RuntimeFilter(CompositeColumnFilter partitionColumnFilter, List<CompositeColumnFilter> nonPartitionColumnFilters, String senderInfo) {
    this(partitionColumnFilter, nonPartitionColumnFilters, senderInfo, Collections.emptyList());
  }

  public RuntimeFilter(CompositeColumnFilter partitionColumnFilter, List<CompositeColumnFilter> nonPartitionColumnFilters, String senderInfo, List<UserBitShared.RunTimeFilterDetailsInfoInScan> filterDetails) {
    this.partitionColumnFilter = partitionColumnFilter;
    this.nonPartitionColumnFilters = nonPartitionColumnFilters;
    this.senderInfo = senderInfo;
    this.filterDetails = filterDetails;
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

  public List<UserBitShared.RunTimeFilterDetailsInfoInScan> getFilterDetails() {
    return filterDetails;
  }

  public static RuntimeFilter getInstance(final ExecProtos.RuntimeFilter protoFilter,
                                          final List<ArrowBuf> buffers,
                                          final String senderInfo,
                                          final String sourceJoinId,
                                          final ExecProtos.FragmentHandle fragmentHandle,
                                          final OperatorStats stats,
                                          final BufferManager manager,
                                          final OptionManager optionManager) {
    ExecProtos.CompositeColumnFilter partitionColFilterProto = protoFilter.getPartitionColumnFilter();
    CompositeColumnFilter partitionColFilter = null;
    List<UserBitShared.RunTimeFilterDetailsInfoInScan> filterDetails = new ArrayList<>();

    int pcFilterCount = (partitionColFilterProto != null && !partitionColFilterProto.getColumnsList().isEmpty()) ? 1 : 0;
    int npcFilterCount = protoFilter.getNonPartitionColumnFilterCount();
    checkNotNull(buffers);
    checkArgument(buffers.size() == pcFilterCount + npcFilterCount,
      "Buffer count does not match total filter count. Buffer count is %s, " +
        "partition column filter count is %s, non-partition column filter count is %s",
      buffers.size(), pcFilterCount, npcFilterCount);

    int idx = 0;  // buffer idx
    if (pcFilterCount == 1) {
      ArrowBuf pcBuffer = buffers.get(idx++); // get buffer for partition column runtime filter
      checkArgument(pcBuffer.capacity() >= partitionColFilterProto.getSizeBytes(), "Invalid filter size. " +
              "Buffer capacity is %s, expected filter size %s", pcBuffer.capacity(), partitionColFilterProto.getSizeBytes());
      UserBitShared.RunTimeFilterDetailsInfoInScan.Builder runTimeFilterDetails = UserBitShared.RunTimeFilterDetailsInfoInScan.newBuilder();
      try {
        final BloomFilter bloomFilter = BloomFilter.prepareFrom(pcBuffer);
        checkState(bloomFilter.getNumBitsSet()==partitionColFilterProto.getValueCount(),
                "BloomFilter value count mismatched. Expected %s, Actual %s", partitionColFilterProto.getValueCount(), bloomFilter.getNumBitsSet());
        partitionColFilter = new CompositeColumnFilter.Builder().setProtoFields(protoFilter.getPartitionColumnFilter())
                .setBloomFilter(bloomFilter).build();
        bloomFilter.getDataBuffer().getReferenceManager().retain();

        runTimeFilterDetails
          .setMinorFragmentId(fragmentHandle.getMinorFragmentId())
          .setJoinSource(sourceJoinId)
          .addAllProbeFieldNames(partitionColFilter.getColumnsList())
          .setIsPartitionedColumn(true)
          .setNumberOfValues(bloomFilter.getNumBitsSet())
          .setNumberOfHashFunctions(bloomFilter.getNumHashFunctions())
          .setOutputRecordsBeforePruning(stats.getRecordsProcessed());

      } catch (Exception e) {
        stats.addLongStat(RUNTIME_COL_FILTER_DROP_COUNT, 1);
        runTimeFilterDetails.setIsDropped(true);
        logger.warn("Error while processing partition column filter from {} : {}", senderInfo, e.getMessage());
      }
      filterDetails.add(runTimeFilterDetails.build());
    }

    final List<CompositeColumnFilter> nonPartitionColFilters = new ArrayList<>(npcFilterCount);
    for (int i = 0; i < npcFilterCount; i++) {
      final ExecProtos.CompositeColumnFilter nonPartitionColFilterProto = protoFilter.getNonPartitionColumnFilter(i);
      final String fieldName = nonPartitionColFilterProto.getColumns(0);
      ArrowBuf npcBuffer = buffers.get(idx++); // get buffer for non-partition column runtime filter
      checkArgument(npcBuffer.capacity() >= nonPartitionColFilterProto.getSizeBytes(),
              "Invalid filter buffer size for non partition col %s.", fieldName);
      UserBitShared.RunTimeFilterDetailsInfoInScan.Builder runTimeFilterDetails = UserBitShared.RunTimeFilterDetailsInfoInScan.newBuilder();
      try {
        final ValueListFilter valueListFilter = ValueListFilterBuilder.fromBuffer(npcBuffer);
        checkState(valueListFilter.getValueCount()==nonPartitionColFilterProto.getValueCount(),
                "ValueListFilter %s count mismatched. Expected %s, found %s", fieldName,
                nonPartitionColFilterProto.getValueCount(), valueListFilter.getValueCount());
        valueListFilter.setFieldName(fieldName);

        final CompositeColumnFilter.Builder nonPartitionColFilterBuilder = new CompositeColumnFilter.Builder()
          .setProtoFields(nonPartitionColFilterProto).setValueList(valueListFilter);

        boolean rowLevelRuntimeFilteringEnable = optionManager.getOption(ExecConstants.ENABLE_ROW_LEVEL_RUNTIME_FILTERING);
        valueListFilter.buf().getReferenceManager().retain();
        if (rowLevelRuntimeFilteringEnable) {
          ArrowBuf buf = manager.getManagedBuffer(valueListFilter.buf().capacity() + ValueListFilter.BLOOM_FILTER_SIZE);
          buf.setZero(0, buf.capacity());
          final ValueListWithBloomFilter valueListFilterWithBloomFilter = ValueListFilterBuilder
            .fromBufferWithBloomFilter(buf, valueListFilter);
          valueListFilterWithBloomFilter.buf().getReferenceManager().retain();

          nonPartitionColFilterBuilder.setValueList(valueListFilterWithBloomFilter);
          valueListFilter.buf().getReferenceManager().release();
        }

        final CompositeColumnFilter nonPartitionColFilter = nonPartitionColFilterBuilder.build();
        nonPartitionColFilters.add(nonPartitionColFilter);

        runTimeFilterDetails
          .setMinorFragmentId(fragmentHandle.getMinorFragmentId())
          .setJoinSource(sourceJoinId)
          .addAllProbeFieldNames(nonPartitionColFilter.getColumnsList())
          .setIsPartitionedColumn(false)
          .setNumberOfValues(nonPartitionColFilter.getValueList().getValueCount())
          .setNumberOfHashFunctions(0)
          .setOutputRecordsBeforePruning(stats.getRecordsProcessed());

      } catch (Exception e) {
        stats.addLongStat(RUNTIME_COL_FILTER_DROP_COUNT, 1);
        runTimeFilterDetails.setIsDropped(true);
        logger.warn("Error while processing non-partition column filter on column {}, from {} : {}",
                protoFilter.getNonPartitionColumnFilter(i).getColumns(0), senderInfo, e.getMessage());
      }
      filterDetails.add(runTimeFilterDetails.build());
    }
    checkState(partitionColFilter != null || !nonPartitionColFilters.isEmpty(), "All filters are dropped.");
    return new RuntimeFilter(partitionColFilter, nonPartitionColFilters, senderInfo, filterDetails);
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
