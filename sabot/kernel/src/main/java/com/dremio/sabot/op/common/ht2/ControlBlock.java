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

import io.netty.buffer.ArrowBuf;

public class ControlBlock implements AutoCloseable {

  private static final int SIZE = LBlockHashTable.MAX_VALUES_PER_BATCH * LBlockHashTable.CONTROL_WIDTH;

  private ArrowBuf buf;

  public ControlBlock(BufferAllocator allocator){
    this.buf = allocator.buffer(SIZE);

  }

  public long getMemoryAddress(){
    return buf.memoryAddress();
  }

  @Override
  public void close() {
    if(buf != null){
      buf.release();
      buf = null;
    }
  }
}
