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
package com.dremio.sabot.op.aggregate.vectorized;

import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.HTORDINAL_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.KEYINDEX_OFFSET;
import static com.dremio.sabot.op.aggregate.vectorized.VectorizedHashAggOperator.PARTITIONINDEX_HTORDINAL_WIDTH;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;

import io.netty.util.internal.PlatformDependent;

public class CountColumnAccumulator extends BaseSingleAccumulator {

  public CountColumnAccumulator(FieldVector input, FieldVector output,
                                FieldVector transferVector, int maxValuesPerBatch,
                                BufferAllocator computationVectorAllocator) {
    super(input, output, transferVector, AccumulatorBuilder.AccumulatorType.COUNT, maxValuesPerBatch,
          computationVectorAllocator);
  }

  public void accumulate(final long memoryAddr, final int count){
    final long maxAddr = memoryAddr + count * PARTITIONINDEX_HTORDINAL_WIDTH;
    final long incomingBit = getInput().getValidityBufferAddress();
    final long[] valueAddresses = this.valueAddresses;
    final int maxValuesPerBatch = super.maxValuesPerBatch;

    for (long partitionAndOrdinalAddr = memoryAddr; partitionAndOrdinalAddr < maxAddr; partitionAndOrdinalAddr += PARTITIONINDEX_HTORDINAL_WIDTH) {
      /* get the hash table ordinal */
      final int tableIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + HTORDINAL_OFFSET);
      /* get the index of data in input vector */
      final int incomingIndex = PlatformDependent.getInt(partitionAndOrdinalAddr + KEYINDEX_OFFSET);
      /* get the corresponding data from input vector -- source data for accumulation */
      final int bitVal = (PlatformDependent.getByte(incomingBit + ((incomingIndex >>> 3))) >>> (incomingIndex & 7)) & 1;
      /* get the target addresses of accumulation vector */
      final int chunkIndex = getChunkIndexForOrdinal(tableIndex, maxValuesPerBatch);
      final int chunkOffset = getOffsetInChunkForOrdinal(tableIndex, maxValuesPerBatch);
      final long countAddr = valueAddresses[chunkIndex] + chunkOffset * 8;
      /* store the accumulated values(count) at the target location of accumulation vector */
      PlatformDependent.putLong(countAddr, PlatformDependent.getLong(countAddr) + bitVal);
    }
  }
}
