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
package com.dremio.exec.record.selection;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.record.DeadBuf;

/**
 * A selection vector that fronts, at most, a
 */
public class SelectionVector2 implements AutoCloseable {
  // private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SelectionVector2.class);

  private final BufferAllocator allocator;
  private int recordCount;
  private ArrowBuf buffer = DeadBuf.DEAD_BUFFER;

  public static final int RECORD_SIZE = 2;

  public SelectionVector2(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public int getCount() {
    return recordCount;
  }

  public ArrowBuf getBuffer() {
    return getBuffer(true);
  }

  public long memoryAddress(){
    return buffer.memoryAddress();
  }

  public ArrowBuf getBuffer(boolean clear) {
    ArrowBuf bufferHandle = this.buffer;

    if (clear) {
      /* Increment the ref count for this buffer */
      bufferHandle.retain(1);

      /* We are passing ownership of the buffer to the
       * caller. clear the buffer from within our selection vector
       */
      clear();
    }

    return bufferHandle;
  }

  public void setBuffer(ArrowBuf bufferHandle) {
      /* clear the existing buffer */
      clear();

      this.buffer = bufferHandle;
      buffer.retain(1);
  }

  public char getIndex(int index) {
    return buffer.getChar(index * RECORD_SIZE);
  }

  public void setIndex(int index, char value) {
    buffer.setChar(index * RECORD_SIZE, value);
  }

  public long getDataAddr() {
    return buffer.memoryAddress();
  }

  public void setIndex(int index, int value) {
    buffer.setChar(index, value);
  }

  public void allocateNew(int size) {
    clear();
    buffer = allocator.buffer(size * RECORD_SIZE);
  }

  @Override
  public SelectionVector2 clone() {
    SelectionVector2 newSV = new SelectionVector2(allocator);
    newSV.recordCount = recordCount;
    newSV.buffer = buffer;

    /* Since buffer and newSV.buffer essentially point to the
     * same buffer, if we don't do a retain() on the newSV's
     * buffer, it might get freed.
     */
    if (newSV.buffer != null) {
      newSV.buffer.retain(1);
    }
    clear();
    return newSV;
  }

  /**
   * Copy reference to buffer from incoming selection vector and copy its record count.
   * @param from selection vector 2
   */
  public void referTo(SelectionVector2 from) {
    this.buffer = from.buffer;
    this.recordCount = from.recordCount;
    from.buffer = DeadBuf.DEAD_BUFFER;
    from.recordCount = 0;
  }

  public void clear() {
    if (buffer != null && buffer != DeadBuf.DEAD_BUFFER) {
      buffer.release();
      buffer = DeadBuf.DEAD_BUFFER;
      recordCount = 0;
    }
  }

  public void setRecordCount(int recordCount){
//    logger.debug("Setting record count to {}", recordCount);
    this.recordCount = recordCount;
  }

  @Override
  public void close() {
    clear();
  }
}
