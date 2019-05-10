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
package com.dremio.sabot.op.aggregate.vectorized.nospill;


import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VariableWidthVector;

import com.dremio.exec.expr.fn.hll.StatisticsAggrFunctions;
import com.dremio.sabot.exec.context.SlicedBufferManager;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;

import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;
import com.yahoo.sketches.hll.Union;

import io.netty.buffer.ArrowBuf;


/**
 * A base accumulator for Union of HLL/NDV operator
 */
abstract class BaseNdvUnionAccumulatorNoSpill implements AccumulatorNoSpill {

  /**
   * holds an array of Union objects. AccumHolder is created for each chunk
   */
  public static class HllUnionAccumHolder {
    private Union[] accums;

    public HllUnionAccumHolder(int count /*number of sketch objects in this holder*/, final SlicedBufferManager bufManager) {
      accums = new Union[count];

      final int size = HllSketch.getMaxUpdatableSerializationBytes(StatisticsAggrFunctions.HLL_ACCURACY, TgtHllType.HLL_8);

      for (int i = 0; i < count; ++i) {
        final ArrowBuf buf = bufManager.getManagedBufferSliced(size);
        buf.setZero(0, size);
        this.accums[i] = new Union(StatisticsAggrFunctions.HLL_ACCURACY, WritableMemory.wrap(buf.nioBuffer(0, size)));
      }
    }

    public Union[] getAccums() {
      return accums;
    }
  }

  protected final FieldVector input;
  protected final FieldVector output;
  protected HllUnionAccumHolder[] accumulators;

  protected final SlicedBufferManager bufManager;

  public BaseNdvUnionAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager){
    this.input = input;
    this.output = output;
    initArrs(0);
    bufManager = (SlicedBufferManager)bufferManager;
  }

  FieldVector getInput(){
    return input;
  }

  private void initArrs(int size){
    this.accumulators = new HllUnionAccumHolder[size];
  }

  @Override
  public void resized(int newCapacity) {
    final int oldBatches = accumulators.length;
    final int currentCapacity = oldBatches * LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH;
    if(currentCapacity >= newCapacity){
      return;
    }

    // save old references.
    final HllUnionAccumHolder[] oldAccumulators = this.accumulators;

    final int newBatches = (int) Math.ceil( newCapacity / (LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH * 1.0d) );
    initArrs(newBatches);

    System.arraycopy(oldAccumulators, 0, this.accumulators, 0, oldBatches);

    for(int i = oldAccumulators.length; i < newBatches; i++){
      accumulators[i] = new HllUnionAccumHolder(LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH, bufManager);
    }
  }

  @Override
  public void output(int batchIndex) {
    HllUnionAccumHolder ah = accumulators[batchIndex];

    Union[] sketches = ah.getAccums();

    int total_size = 0;
    for (int i = 0; i < sketches.length; ++i) {
      total_size += sketches[i].getCompactSerializationBytes();
    }

    ((VariableWidthVector) output).allocateNew(total_size, LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH);
    VarBinaryVector outVec = (VarBinaryVector) output;

    for (int i = 0; i < sketches.length; ++i) {
      byte[] ba = sketches[i].toCompactByteArray();
      outVec.setSafe(i, ba, 0, ba.length);
    }
  }

  /*
   * bufferManger is being closed as part of OperatorContext close
   */
  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception { }

}
