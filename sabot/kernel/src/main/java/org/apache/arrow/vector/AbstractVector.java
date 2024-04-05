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

package org.apache.arrow.vector;

import com.google.common.base.Preconditions;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.util.CommonUtil;
import org.apache.arrow.vector.util.OversizedAllocationException;

/**
 * A minimal stub over ArrowBuf for simple usages of non-nullable scalar vector like functions --
 * get, set, etc. An example usage is the deltas vector in Parquet reader where we want to perform
 * get(), set(), getValueCount(), setValueCount() functions on integer elements in ArrowBuf.
 */
public abstract class AbstractVector implements AutoCloseable {
  public static final int INITIAL_VALUE_ALLOCATION = 4096;
  public static final int MAX_ALLOCATION_SIZE =
      Integer.getInteger("max_bytes", Integer.MAX_VALUE).intValue();
  private int allocationSizeInBytes;
  private final String name;
  protected final BufferAllocator allocator;
  protected int valueCount;
  protected ArrowBuf dataBuffer;
  private final int typeWidth;

  public AbstractVector(String name, BufferAllocator allocator, int typeWidth) {
    this.name = name;
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.typeWidth = typeWidth;
    valueCount = 0;
    allocationSizeInBytes = INITIAL_VALUE_ALLOCATION * typeWidth;
    dataBuffer = allocator.getEmpty();
  }

  public int getValueCapacity() {
    return (int) ((dataBuffer.capacity() * 1.0) / typeWidth);
  }

  public void setInitialCapacity(int valueCount) {
    long size = 1L * (long) valueCount * typeWidth;
    if (size > (long) MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException(
          "Requested amount of memory is more than max allowed allocation size");
    } else {
      this.allocationSizeInBytes = (int) size;
    }
  }

  public void allocateNew() {
    clear();
    this.allocateBytes((long) allocationSizeInBytes);
  }

  public void allocateNew(int valueCount) {
    clear();
    this.allocateBytes((long) (valueCount * typeWidth));
  }

  public void reset() {
    this.zeroVector();
    valueCount = 0;
  }

  private void allocateBytes(long size) {
    if (size > (long) MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException(
          "Requested amount of memory is more than max allowed allocation size");
    } else {
      int curSize = (int) size;
      clear();
      dataBuffer = this.allocator.buffer(curSize);
      dataBuffer.readerIndex(0);
      allocationSizeInBytes = (curSize > 0) ? curSize : allocationSizeInBytes;
    }
  }

  public void clear() {
    dataBuffer.close();
    dataBuffer = allocator.getEmpty();
    valueCount = 0;
  }

  @Override
  public void close() {
    clear();
  }

  /**
   * Allocs max(currentCapacity * 2, prevAllocSize, defaultAllocSize) If this is called after
   * clear(), allocs allocationSizeInBytes which is the size of previous allocation
   */
  public void reAlloc() {
    long currentBufferCapacity = dataBuffer.capacity();
    long newAllocationSize = currentBufferCapacity * 2L;

    if (newAllocationSize == 0) {
      if (allocationSizeInBytes > 0) {
        newAllocationSize = allocationSizeInBytes;
      } else {
        newAllocationSize = INITIAL_VALUE_ALLOCATION * typeWidth;
      }
    }

    newAllocationSize = CommonUtil.nextPowerOfTwo(newAllocationSize);
    if (newAllocationSize > (long) MAX_ALLOCATION_SIZE) {
      throw new OversizedAllocationException(
          "Unable to expand the buffer. Max allowed buffer size is reached.");
    } else {
      ArrowBuf newBuf = this.allocator.buffer((int) newAllocationSize);
      newBuf.setZero(0, newBuf.capacity());
      newBuf.setBytes(0, dataBuffer, 0, currentBufferCapacity);
      newBuf.writerIndex(dataBuffer.writerIndex());
      dataBuffer.getReferenceManager().release(1);
      dataBuffer = newBuf;
      allocationSizeInBytes = (int) newAllocationSize;
    }
  }

  public void zeroVector() {
    dataBuffer.setZero(0, dataBuffer.capacity());
  }

  public void setValueCount(int valueCount) {
    this.valueCount = valueCount;
    while (valueCount > getValueCapacity()) {
      reAlloc();
    }
    dataBuffer.writerIndex(valueCount * typeWidth);
  }

  public int getValueCount() {
    return valueCount;
  }

  public void transferTo(SimpleIntVector target) {
    target.clear();
    target.dataBuffer =
        this.dataBuffer
            .getReferenceManager()
            .transferOwnership(this.dataBuffer, target.allocator)
            .getTransferredBuffer();
    target.dataBuffer.writerIndex(this.dataBuffer.writerIndex());
    this.clear();
  }

  public long getBufferAddress() {
    return dataBuffer.memoryAddress();
  }
}
