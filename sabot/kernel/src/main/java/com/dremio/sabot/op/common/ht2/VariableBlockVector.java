/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.util.Numbers;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class VariableBlockVector implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int fieldCount;
  private ArrowBuf buf;

  public VariableBlockVector(BufferAllocator allocator, int fieldCount) {
    super();
    this.allocator = allocator;
    this.fieldCount = fieldCount;
    this.buf = allocator.buffer(0);
  }

  public long getMemoryAddress(){
    return buf.memoryAddress();
  }

  public long getMaxMemoryAddress(){
    return buf.memoryAddress() + buf.capacity();
  }

  public int getVariableFieldCount(){
    return fieldCount;
  }

  /**
   * Expand the buffer as necessary.
   * @param sizeInBytes
   * @return true if the buffer was expanded (meaning one needs to reread the memory address).
   */
  public boolean ensureAvailableDataSpace(int sizeInBytes){
    if(buf.capacity() < sizeInBytes){
      int targetSize = Numbers.nextPowerOfTwo(sizeInBytes);
      final ArrowBuf oldBuf = buf;
      buf = allocator.buffer(targetSize);
      PlatformDependent.copyMemory(oldBuf.memoryAddress(), buf.memoryAddress(), oldBuf.capacity());
      oldBuf.release();
      return true;
    }

    return false;
  }

  @VisibleForTesting
  ArrowBuf getUnderlying(){
    return buf;
  }

  @Override
  public synchronized void close() {
    if(buf != null){
      buf.release();
      buf = null;
    }
  }

}
