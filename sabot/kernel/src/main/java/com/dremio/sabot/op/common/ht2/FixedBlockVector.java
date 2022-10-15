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
package com.dremio.sabot.op.common.ht2;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.LargeMemoryUtil;

import com.dremio.common.util.Numbers;
import com.google.common.annotations.VisibleForTesting;

import io.netty.util.internal.PlatformDependent;

public class FixedBlockVector implements AutoCloseable {

  private final BufferAllocator allocator;
  private final int blockWidth;
  private final boolean allowExpansion;
  private ArrowBuf buf;
  private int capacity;

  public FixedBlockVector(BufferAllocator allocator, int blockWidth) {
    this(allocator, blockWidth, 0, true);
  }

  public FixedBlockVector(BufferAllocator allocator, int blockWidth, int initialCapacity, boolean allowExpansion) {
    this.allocator = allocator;
    this.blockWidth = blockWidth;
    this.allowExpansion = allowExpansion;
    this.buf = allocator.buffer(0);
    this.capacity = 0;
    resizeBuffer(initialCapacity);
    resetPositions();
  }

  public ArrowBuf getBuf() {
    return buf;
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
    buf.close();
    buf = allocator.buffer(count * blockWidth);
    resetPositions();
  }

  public void ensureAvailableBlocks(int count) {
    if (count > capacity) {
      if (!allowExpansion) {
        throw new RuntimeException("This buffer has fixed capacity. Not allowed to expand");
      }
      resizeBuffer(count);
    }
  }

  // Compute the direct memory required for one fixed block.
  public static int computeSizeForSingleBlock(final int batchSize, final int blockWidth) {
    return Numbers.nextPowerOfTwo(Numbers.nextPowerOfTwo(batchSize) * blockWidth);
  }

  private void resizeBuffer(int newCapacity) {
    final int blockWidth = this.blockWidth;
    final int sizeInBytes = Numbers.nextPowerOfTwo(newCapacity * blockWidth);
    final ArrowBuf oldBuf = buf;
    buf = allocator.buffer(sizeInBytes);
    final int oldCapacity = this.capacity;

    fillZeros(buf.memoryAddress() + oldCapacity * blockWidth, (newCapacity - oldCapacity) * blockWidth);

    PlatformDependent.copyMemory(oldBuf.memoryAddress(), buf.memoryAddress(), oldCapacity * blockWidth);

    buf.writerIndex(oldBuf.writerIndex());
    oldBuf.close();
    this.capacity = newCapacity;
  }

  private void fillZeros(long startAddr, int length) {
    long c = startAddr;
    final long endAddr = startAddr + length;
    while(c < endAddr) {
      PlatformDependent.putLong(c, 0);
      c += 8;
    }

    int remain = length % 8;
    if(remain != 0){
      for(int i = 0; i < remain ; i++) {
        PlatformDependent.putByte(endAddr - i, (byte)0);
      }
    }
  }

  public void reset() {
    resetPositions();
    buf.setZero(0, buf.capacity());
  }

  @VisibleForTesting
  ArrowBuf getUnderlying(){
    return buf;
  }

  @Override
  public synchronized void close() {
    if(buf != null){
      buf.close();
      buf = null;
    }
  }

  private void resetPositions() {
    buf.readerIndex(0);
    buf.writerIndex(0);
  }

  public int getBufferLength() {
    return LargeMemoryUtil.checkedCastToInt(buf.writerIndex());
  }

  public int getCapacity() {
    return buf != null ? LargeMemoryUtil.checkedCastToInt(buf.capacity()) : 0;
  }
}
