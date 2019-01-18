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
package com.dremio.exec.expr.fn.hll;

import java.nio.ByteBuffer;

import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.vector.holders.ObjectHolder;

import com.yahoo.memory.Memory;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.hll.HllSketch;
import com.yahoo.sketches.hll.TgtHllType;

import io.netty.buffer.ArrowBuf;

/**
 * Holding class for HLL Accumulation object. Implement this abstraction to keep UDFs simple.
 */
@SuppressWarnings("deprecation")
public final class HLLAccum {

  private final ArrowBuf buf;
  private final HllSketch sketch;

  private HLLAccum(BufferManager manager, int lgConfigK) {
    final int size = HllSketch.getMaxUpdatableSerializationBytes(lgConfigK, TgtHllType.HLL_8);
    this.buf = manager.getManagedBuffer(size);
    buf.setZero(0, size);
    this.sketch = new HllSketch(lgConfigK, TgtHllType.HLL_8, WritableMemory.wrap(buf.nioBuffer(0, size)));
  }

  public void addInt(int value) {
    sketch.update(value);
  }

  public void addLong(long value) {
    sketch.update(value);
  }

  public void addDouble(double value) {
    sketch.update(value);
  }

  public void addFloat(float value) {
    sketch.update(value);
  }

  public void addBytes(final ArrowBuf buf, final int start, final int end) {
    final int len = end - start;
    ByteBuffer buffer = buf.nioBuffer(start, len);
    Memory.wrap(buffer);
    sketch.update(Memory.wrap(buffer), 0, len);
  }

  public byte[] getOutputBytes() {
    return sketch.toCompactByteArray();
  }

  public long getCardinality() {
    return (long) sketch.getEstimate();
  }

  @SuppressWarnings("deprecation")
  public static ObjectHolder create(ObjectHolder holder, BufferManager manager) {
    if(holder == null) {
      holder = new ObjectHolder();
    }

    if(holder.obj == null) {
      holder.obj = new HLLAccum(manager, StatisticsAggrFunctions.HLL_ACCURACY);
    }
    return holder;
  }

  public static long getEstimate(ArrowBuf buf, int start, int end) {
    return (long) HllSketch.wrap(Memory.wrap(buf.nioBuffer(start, end - start))).getEstimate();
  }


  public void reset() {
    sketch.reset();
  }

}
