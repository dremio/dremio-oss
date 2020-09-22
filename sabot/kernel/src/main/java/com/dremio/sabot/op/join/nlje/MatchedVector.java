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
package com.dremio.sabot.op.join.nlje;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVectorHelper;

import com.dremio.common.util.Closeable;

class MatchedVector implements Closeable {
  private ArrowBuf buffer;
  private int size;

  public MatchedVector(BufferAllocator allocator) {
    this.buffer = allocator.buffer(4096);
  }

  public boolean isSet(int index) {
    return buffer.getByte(index) == 0;
  }

  public int count() {
    // we'll use bit vector helper since we only set one bit per byte.
    return BitVectorHelper.getNullCount(buffer, size * 8 /** hack so bit vector helper thinks we have an 8 times larger validity vector **/);
  }

  public void zero(int size) {
    if(buffer.capacity() < size) {
      this.buffer.getReferenceManager().release();
      this.buffer = buffer.getReferenceManager().getAllocator().buffer(size);
    }
    this.size = size;
    this.buffer.setZero(0, size);
  }

  public void mark(int index) {
    buffer.setByte(index, 1);
  }

  @Override
  public void close() {
    buffer.getReferenceManager().release();
  }
}
