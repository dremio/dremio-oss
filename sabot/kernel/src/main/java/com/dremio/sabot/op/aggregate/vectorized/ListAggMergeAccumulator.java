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
package com.dremio.sabot.op.aggregate.vectorized;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BaseValueVector;
import org.apache.arrow.vector.FieldVector;

/*
 * This is the aggregator for LISTAGG_MERGE, similar to other aggregate accumulators.
 * This accumulator is used in the second phase of the HashAgg.
 */
public class ListAggMergeAccumulator extends ListAggAccumulator {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ListAggMergeAccumulator.class);
  private final boolean outputToListVector;

  public ListAggMergeAccumulator(
      ListAggAccumulator listAggAccumulator,
      FieldVector incomingValues,
      int maxValuesPerBatch,
      BufferAllocator computationVectorAllocator,
      boolean outputToListVector) {
    super(
        incomingValues,
        listAggAccumulator.getOutput(),
        maxValuesPerBatch,
        computationVectorAllocator,
        new AccumulatorBuilder.ListAggParams(
            listAggAccumulator.getMaxListAggSize(),
            listAggAccumulator.getDistinct(),
            listAggAccumulator.getDelimiter(),
            listAggAccumulator.getOrderBy(),
            listAggAccumulator.getAsc()),
        listAggAccumulator.getTempAccumulator(),
        listAggAccumulator.getFirstAccumulator(),
        listAggAccumulator.getAccumStats());
    this.outputToListVector = outputToListVector;
  }

  public ListAggMergeAccumulator(
      FieldVector incomingValues,
      FieldVector transferVector,
      int maxValuesPerBatch,
      BufferAllocator computationVectorAllocator,
      AccumulatorBuilder.ListAggParams listAggParams,
      BaseValueVector tempAccumulator) {
    super(
        incomingValues,
        transferVector,
        maxValuesPerBatch,
        computationVectorAllocator,
        listAggParams,
        tempAccumulator);
    this.outputToListVector = false;
  }

  @Override
  public AccumulatorBuilder.AccumulatorType getType() {
    return AccumulatorBuilder.AccumulatorType.LISTAGG_MERGE;
  }

  @Override
  public void output(int startBatchIndex, int[] recordsInBatches) {
    if (outputToListVector) {
      outputToListVector(startBatchIndex, recordsInBatches);
    } else {
      outputToVarWidthVector(startBatchIndex, recordsInBatches);
    }
  }

  @Override
  public void accumulate(
      final long memoryAddr, final int count, final int bitsInChunk, final int chunkOffsetMask) {
    accumulateFromListVector(memoryAddr, count, bitsInChunk, chunkOffsetMask);
  }
}
