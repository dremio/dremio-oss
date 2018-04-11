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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.util.Numbers;
import com.google.common.annotations.VisibleForTesting;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class FixedBlockVector implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int blockWidth;
  private ArrowBuf buf;
  private int capacity;

  public FixedBlockVector(BufferAllocator allocator, int blockWidth) {
    super();
    this.allocator = allocator;
    this.blockWidth = blockWidth;
    this.buf = allocator.buffer(0);
    this.capacity = 0;
  }

  public long getMemoryAddress(){
    return buf.memoryAddress();
  }

  public long getMaxMemoryAddress() {
    return buf.memoryAddress() + buf.capacity();
  }

  public int getBlockWidth(){
    return blockWidth;
  }

  public void allocateNoClear(int count){
    buf.release();
    buf = allocator.buffer(count * blockWidth);
  }

  public void ensureAvailableBlocks(int count){
    if(count > capacity){
      final int blockWidth = this.blockWidth;
      final int sizeInBytes = Numbers.nextPowerOfTwo(count * blockWidth);
      final ArrowBuf oldBuf = buf;
      buf = allocator.buffer(sizeInBytes);

      // since blockWidth has to be a power of two and count
      final long firstBlock = buf.memoryAddress() + (capacity * blockWidth);
      final int maxBytes = blockWidth * count;
      final long maxBlock = buf.memoryAddress() + maxBytes;
      for(long l = firstBlock; l < maxBlock; l+= 8){
        PlatformDependent.putLong(l, 0);
      }

      int remain = maxBytes % 8;
      if(remain != 0){
        buf.setZero(maxBytes - remain, remain);
      }

      PlatformDependent.copyMemory(oldBuf.memoryAddress(), buf.memoryAddress(), capacity * blockWidth);

      oldBuf.release();
      this.capacity = count;
    }
  }

  @VisibleForTesting
  ArrowBuf getUnderlying(){
    return buf;
  }

  public byte[] asBytes(int count){
    byte[] bytes = new byte[count * blockWidth];
    buf.getBytes(0, bytes);
    return bytes;
  }

  @Override
  public synchronized void close() {
    if(buf != null){
      buf.release();
      buf = null;
    }
  }


}
