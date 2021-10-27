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


import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.util.MemoryUtil;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VariableWidthVector;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.memory.WritableMemory;

import com.dremio.exec.expr.fn.hll.StatisticsAggrFunctions;
import com.dremio.sabot.exec.context.SlicedBufferManager;
import com.dremio.sabot.op.common.ht2.LBlockHashTableNoSpill;

/**
 * A base accumulator for HLL/NDV operator
 */
abstract class BaseNdvAccumulatorNoSpill implements AccumulatorNoSpill {

  /**
   * holds an array of memory addresses, each is backing memory for a single sketch or actual HllSketch objects. AccumHolder is created for each chunk
   */
  public static class HllAccumHolder {
    /*
     * ArrowBuf size must be power of 2. 2M is ideal size due to the odd size of sketchSize (4136).
     * (There is only total of 200 bytes of direct memory will be wasted for each 2M size. If default
     * Sliced buffer size (64K) is used, there will be 3496 bytes (6%) wastage for each ArrowBuf).
     * Also large ArrowBuf means, less number of ByteBuffers to be maintained.
     */
    static private final int SKETCH_BUF_SIZE = 2 * 1024 * 1024;
    static private final int sketchSize = HllSketch.getMaxUpdatableSerializationBytes(StatisticsAggrFunctions.HLL_ACCURACY, TgtHllType.HLL_8);
    static private final int SKETCHES_PER_BUF = SKETCH_BUF_SIZE / sketchSize;
    private boolean reduceNdvHeap;
    private final int count;
    private ByteBuffer[] accumAddresses;
    private HllSketch[] accumObjs;


    public HllAccumHolder(int count /* number of sketch objects in this holder */, final SlicedBufferManager bufManager, boolean reduceNdvHeap) {
      this.count = count;
      int numArrowBufs = (count + SKETCHES_PER_BUF - 1) / SKETCHES_PER_BUF;
      accumAddresses = new ByteBuffer[numArrowBufs];
      for (int i = 0; i < numArrowBufs; ++i) {
        int bufSize = SKETCH_BUF_SIZE;
        if (i == numArrowBufs - 1) {
          /* Adjust the buffer size for the last arrow buf */
          bufSize = (count - i * SKETCHES_PER_BUF) * sketchSize;
        }
        accumAddresses[i] = MemoryUtil.directBuffer(bufManager.getManagedBufferSliced(bufSize).memoryAddress(), bufSize);
      }

      this.reduceNdvHeap = reduceNdvHeap;
      if (!reduceNdvHeap) {
        accumObjs = new HllSketch[count];
      }

      for (int i = 0; i < count; ++i) {
        int arrowBufIndex = i / SKETCHES_PER_BUF;
        int arrowBufOffset = (i % SKETCHES_PER_BUF) * sketchSize;
        accumAddresses[arrowBufIndex].limit(arrowBufOffset + sketchSize);
        accumAddresses[arrowBufIndex].position(arrowBufOffset);
        ByteBuffer bb = accumAddresses[arrowBufIndex].slice();
        bb.order(ByteOrder.nativeOrder());

        /* Initialize backing memory for the sketch. HllSketch memset first getMaxUpdatableSerializationBytes() bytes. */
        HllSketch sketch = new HllSketch(StatisticsAggrFunctions.HLL_ACCURACY, TgtHllType.HLL_8, WritableMemory.wrap(bb));
        if (!reduceNdvHeap) {
          accumObjs[i] = sketch;
        }
      }
    }

    public int getAccumsSize() {
      return count;
    }

    public HllSketch getAccumSketch(int accumIndex) {
      Preconditions.checkArgument(accumIndex < getAccumsSize());
      if (reduceNdvHeap) {
        int arrowBufIndex = accumIndex / SKETCHES_PER_BUF;
        int arrowBufOffset = (accumIndex % SKETCHES_PER_BUF) * sketchSize;
        accumAddresses[arrowBufIndex].limit(arrowBufOffset + sketchSize);
        accumAddresses[arrowBufIndex].position(arrowBufOffset);
        ByteBuffer bb = accumAddresses[arrowBufIndex].slice();
        bb.order(ByteOrder.nativeOrder());
        return HllSketch.writableWrap(WritableMemory.wrap(bb));
      } else {
        return accumObjs[accumIndex];
      }
    }
  }

  protected final boolean reduceNdvHeap;
  protected final FieldVector input;
  protected final FieldVector output;
  protected HllAccumHolder[] accumulators;
  protected final SlicedBufferManager bufManager;

  public BaseNdvAccumulatorNoSpill(FieldVector input, FieldVector output, BufferManager bufferManager, boolean reduceNdvHeap) {
    this.input = input;
    this.output = output;
    initArrs(0);
    bufManager = (SlicedBufferManager)bufferManager;
    this.reduceNdvHeap = reduceNdvHeap;
  }

  FieldVector getInput(){
    return input;
  }

  private void initArrs(int size){
    this.accumulators = new HllAccumHolder[size];
  }

  @Override
  public void resized(int newCapacity) {
    final int oldBatches = accumulators.length;
    final int currentCapacity = oldBatches * LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH;
    if(currentCapacity >= newCapacity){
      return;
    }

    // save old references.
    final HllAccumHolder[] oldAccumulators = this.accumulators;

    final int newBatches = (int) Math.ceil( newCapacity / (LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH * 1.0d) );
    initArrs(newBatches);

    System.arraycopy(oldAccumulators, 0, this.accumulators, 0, oldBatches);

    for (int i = oldAccumulators.length; i < newBatches; i++) {
      accumulators[i] = new HllAccumHolder(LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH, bufManager, reduceNdvHeap);
    }
  }

  @Override
  public void output(int batchIndex) {
    HllAccumHolder ah = accumulators[batchIndex];
    int batchSize = ah.getAccumsSize();

    int total_size = 0;
    for (int i = 0; i < batchSize; ++i) {
      HllSketch sketch = ah.getAccumSketch(i);
      total_size += sketch.getCompactSerializationBytes();
    }

    ((VariableWidthVector) output).allocateNew(total_size, LBlockHashTableNoSpill.MAX_VALUES_PER_BATCH);
    VarBinaryVector outVec = (VarBinaryVector) output;

    for (int i = 0; i < batchSize; ++i) {
      HllSketch sketch = ah.getAccumSketch(i);
      byte[] ba = sketch.toCompactByteArray();
      outVec.set(i, ba);
    }
  }

  /*
   * bufferManger is being closed as part of OperatorContext close
   */
  @SuppressWarnings("unchecked")
  @Override
  public void close() throws Exception { }

}
