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

import org.apache.arrow.memory.BufferAllocator;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

public class ControlBlock implements AutoCloseable {
  private ArrowBuf buf;

  public ControlBlock(final BufferAllocator allocator, final int maxValuesPerBatch){
    this.buf = allocator.buffer(maxValuesPerBatch * LBlockHashTable.CONTROL_WIDTH);

  }

  public long getMemoryAddress(){
    return buf.memoryAddress();
  }

  public void reset() {
    if (buf != null) {
      buf.readerIndex(0);
      buf.writerIndex(0);

      long c = buf.memoryAddress();
      final long endAddr = c + buf.capacity();
      while(c < endAddr) {
        PlatformDependent.putLong(c, 0);
        c += 8;
      }

      int remain = buf.capacity() % 8;
      if(remain != 0){
        for(int i = 0; i < remain ; i++) {
          PlatformDependent.putByte(endAddr - i, (byte)0);
        }
      }
    }
  }

  @Override
  public void close() {
    if(buf != null){
      buf.release();
      buf = null;
    }
  }
}
