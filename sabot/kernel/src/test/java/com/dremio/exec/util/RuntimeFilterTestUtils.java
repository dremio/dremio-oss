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

package com.dremio.exec.util;

import com.dremio.exec.proto.ExecProtos;
import com.dremio.sabot.exec.fragment.OutOfBandMessage;
import com.dremio.sabot.op.common.ht2.Copier;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.Types;

/** Test utilities */
public class RuntimeFilterTestUtils {

  private BufferAllocator testAllocator;

  public RuntimeFilterTestUtils(BufferAllocator testAllocator) {
    this.testAllocator = testAllocator;
  }

  public OutOfBandMessage newOOB(
      int sendingMajorFragment,
      int sendingOperator,
      int sendingMinorFragment,
      List<String> partitionCols,
      ArrowBuf bloomFilterBuf,
      ValueListFilter... nonPartitionColFilters) {
    List<Integer> allFragments = Lists.newArrayList(1, 2, 3, 4);
    allFragments.removeIf(val -> val == sendingMinorFragment);
    List<Integer> bufferLengths = new ArrayList<>();

    ExecProtos.RuntimeFilter.Builder runtimeFilter =
        ExecProtos.RuntimeFilter.newBuilder()
            .setProbeScanOperatorId(101)
            .setProbeScanMajorFragmentId(1);
    List<ArrowBuf> bufsToMerge = new ArrayList<>(nonPartitionColFilters.length + 1);
    if (!partitionCols.isEmpty()) {
      ExecProtos.CompositeColumnFilter partitionColFilter =
          ExecProtos.CompositeColumnFilter.newBuilder()
              .setFilterType(ExecProtos.RuntimeFilterType.BLOOM_FILTER)
              .setSizeBytes(bloomFilterBuf.capacity())
              .setValueCount(0)
              .addAllColumns(partitionCols)
              .build();
      runtimeFilter.setPartitionColumnFilter(partitionColFilter);
      bloomFilterBuf.readerIndex(0);
      bloomFilterBuf.writerIndex(bloomFilterBuf.capacity());
      bufsToMerge.add(bloomFilterBuf);
      bufferLengths.add((int) runtimeFilter.getPartitionColumnFilter().getSizeBytes());
    }

    if (nonPartitionColFilters.length > 0) {
      for (ValueListFilter vlf : nonPartitionColFilters) {
        ExecProtos.CompositeColumnFilter nonPartitionColFilter =
            ExecProtos.CompositeColumnFilter.newBuilder()
                .setFilterType(ExecProtos.RuntimeFilterType.VALUE_LIST)
                .setSizeBytes(vlf.getSizeInBytes())
                .setValueCount(vlf.getValueCount())
                .addColumns(vlf.getFieldName())
                .build();
        runtimeFilter.addNonPartitionColumnFilter(nonPartitionColFilter);
        bufsToMerge.add(vlf.buf());
      }
      runtimeFilter
          .getNonPartitionColumnFilterList()
          .forEach(v -> bufferLengths.add((int) v.getSizeBytes()));
    }
    ArrowBuf mergedBuf = getMergedBuf(bufsToMerge);
    final int targetMajorFragment = 1;
    final int targetOperator = 1001;
    OutOfBandMessage msg =
        new OutOfBandMessage(
            null,
            targetMajorFragment,
            allFragments,
            targetOperator,
            sendingMajorFragment,
            sendingMinorFragment,
            sendingOperator,
            new OutOfBandMessage.Payload(runtimeFilter.build()),
            new ArrowBuf[] {mergedBuf},
            bufferLengths,
            true);
    msg.getBuffers()[0].close(); // Compensate for retain in this constructor
    return msg;
  }

  public ValueListFilter prepareNewValueListFilter(
      String fieldName, boolean insertNull, int... values) throws Exception {
    try (ValueListFilterBuilder valueListFilterBuilder =
        new ValueListFilterBuilder(testAllocator, 1024, (byte) 4, false)) {
      valueListFilterBuilder.setup();
      valueListFilterBuilder.setFieldType(Types.MinorType.INT);
      valueListFilterBuilder.setName(fieldName);
      valueListFilterBuilder.setFieldName(fieldName);

      try (ArrowBuf keyBuf = testAllocator.buffer(4)) {
        for (int val : values) {
          keyBuf.setInt(0, val);
          valueListFilterBuilder.insert(keyBuf);
        }
      }
      if (insertNull) {
        valueListFilterBuilder.insertNull();
      }
      return valueListFilterBuilder.build();
    }
  }

  public ValueListFilter prepareNewValueListBooleanFilter(
      String fieldName, boolean insertNull, boolean insertFalse, boolean insertTrue)
      throws Exception {
    try (ValueListFilterBuilder valueListFilterBuilder =
        new ValueListFilterBuilder(testAllocator, 31, (byte) 0, true)) {
      valueListFilterBuilder.setup();
      valueListFilterBuilder.setFieldType(Types.MinorType.BIT);
      valueListFilterBuilder.setName(fieldName);
      valueListFilterBuilder.setFieldName(fieldName);

      if (insertNull) {
        valueListFilterBuilder.insertNull();
      }
      if (insertFalse) {
        valueListFilterBuilder.insertBooleanVal(false);
      }
      if (insertTrue) {
        valueListFilterBuilder.insertBooleanVal(true);
      }
      return valueListFilterBuilder.build();
    }
  }

  public ArrowBuf getMergedBuf(List<ArrowBuf> bufs) {
    long neededSize = bufs.stream().map(ArrowBuf::writerIndex).reduce(0L, Long::sum);
    ArrowBuf mergedBuf = testAllocator.buffer(neededSize);
    long runningIdx = 0;
    for (ArrowBuf buf : bufs) {
      int len = (int) buf.writerIndex();
      Copier.copy(buf.memoryAddress(), mergedBuf.memoryAddress() + runningIdx, len);
      runningIdx += len;
      buf.close();
    }

    return mergedBuf;
  }

  public List<Integer> getValues(ValueListFilter valueListFilter) {
    List<Integer> vals = new ArrayList<>(valueListFilter.getValueCount());
    ArrowBuf valueBuf = valueListFilter.valOnlyBuf();
    for (int i = 0; i < valueListFilter.getValueCount(); i++) {
      vals.add(valueBuf.getInt(i * valueListFilter.getBlockSize()));
    }
    return vals;
  }
}
