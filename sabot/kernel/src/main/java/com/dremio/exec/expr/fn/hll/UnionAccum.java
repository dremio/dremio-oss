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
package com.dremio.exec.expr.fn.hll;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ObjectHolder;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.WritableMemory;

import com.dremio.sabot.exec.context.SlicedBufferManager;

/**
 * An accumulator that designed to combine one or more separate HLL values into a single HLL
 * structure. This has nothing to do with the Union type.
 */
public class UnionAccum {

  private final Union union;

  private UnionAccum(BufferManager manager, int lgConfigK) {
    final int size = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, TgtHllType.HLL_8);
    final ArrowBuf buf = ((SlicedBufferManager) manager).getManagedBufferSliced(size);
    byte[] bytes = new byte[size];
    buf.getBytes(0, bytes);
    this.union = new Union(lgConfigK, WritableMemory.wrap(bytes));
  }

  public void reset() {
    this.union.reset();
  }

  public void addHll(ArrowBuf buf, int start, int end) {
    final int len = end - start;
    byte[] bytes = new byte[len];
    buf.getBytes(start, bytes);
    HllSketch sketch = HllSketch.heapify(bytes);
    union.update(sketch);
  }

  public byte[] getOutputBytes() {
    return union.toCompactByteArray();
  }

  public long getCardinality() {
    return (long) union.getEstimate();
  }

  @SuppressWarnings("deprecation")
  public static ObjectHolder create(ObjectHolder holder, BufferManager manager) {
    if(holder == null) {
      holder = new ObjectHolder();
    }

    if(holder.obj == null) {
      holder.obj = new UnionAccum(manager, StatisticsAggrFunctions.HLL_ACCURACY);
    }
    return holder;
  }
}
