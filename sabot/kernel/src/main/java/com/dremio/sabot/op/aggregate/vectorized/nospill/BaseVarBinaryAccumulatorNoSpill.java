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
package com.dremio.sabot.op.aggregate.vectorized.nospill;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;
import com.google.common.collect.ImmutableList;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.MutableVarcharVector;
import org.apache.arrow.vector.VariableWidthVector;

/**
 * A base accumulator that manages the basic concepts of expanding the array of accumulation vectors
 * associated with the current aggregation. Especially written for mutablevarchar vector. Currently
 * being used when the query has min/max on the var length column.
 */
abstract class BaseVarBinaryAccumulatorNoSpill implements AccumulatorNoSpill {

  protected final FieldVector input;
  protected final FieldVector output;
  protected FieldVector[] accumulators;
  protected int batches;

  public BaseVarBinaryAccumulatorNoSpill(FieldVector input, FieldVector output) {
    this.input = input;
    this.output = output;
    initArrs(0);
    batches = 0;
  }

  FieldVector getInput() {
    return input;
  }

  private void initArrs(int size) {
    this.accumulators = new FieldVector[size];
  }

  @Override
  public void resized(int newCapacity) {
    final int oldBatches = accumulators.length;
    final int currentCapacity = oldBatches * LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH;
    if (currentCapacity >= newCapacity) {
      return;
    }

    // save old references.
    final FieldVector[] oldAccumulators = this.accumulators;

    final int newBatches =
        (int) Math.ceil(newCapacity / (LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH * 1.0d));
    initArrs(newBatches);

    System.arraycopy(oldAccumulators, 0, this.accumulators, 0, oldBatches);

    for (int i = oldAccumulators.length; i < newBatches; i++) {
      accumulators[i] =
          new MutableVarcharVector(input.getField().getName(), input.getAllocator(), 0.5);
      ++batches;
    }
  }

  @Override
  public void output(int batchIndex) {
    MutableVarcharVector mv = (MutableVarcharVector) accumulators[batchIndex];
    ((VariableWidthVector) output)
        .allocateNew(mv.getUsedByteCapacity(), LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH);

    mv.copyToVarWidthVec(
        (BaseVariableWidthVector) output, LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH, 0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception {
    final FieldVector[] accumulatorsToClose = new FieldVector[batches];
    for (int i = 0; i < batches; i++) {
      /* if we earlier failed to resize and hit OOM, we would have NULL(s)
       * towards the end of accumulator array and we need to ignore all of
       * them else the close() call will hit NPE. this is why we loop only
       * until batches.
       */
      accumulatorsToClose[i] = accumulators[i];
    }
    AutoCloseables.close(ImmutableList.copyOf(accumulatorsToClose));
  }
}
