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
package com.dremio.exec.store.hive.exec;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.orc.impl.RecordReaderUtils;

import io.netty.buffer.ArrowBuf;

/**
 *
 */
class HiveORCZeroCopyShim implements org.apache.orc.Reader.ZeroCopyPoolShim {
  private static final class ByteBufferWrapper {
    private final ByteBuffer byteBuffer;

    ByteBufferWrapper(ByteBuffer byteBuffer) {
      this.byteBuffer = byteBuffer;
    }

    @Override
    public boolean equals(Object rhs) {
      return (rhs instanceof ByteBufferWrapper) && (this.byteBuffer == ((ByteBufferWrapper) rhs).byteBuffer);
    }

    @Override
    public int hashCode() {
      return System.identityHashCode(byteBuffer);
    }
  }

  private final Map<ByteBufferWrapper, ArrowBuf> directBufMap = new HashMap<>();
  private final BufferAllocator allocator;
  private final RecordReaderUtils.ByteBufferAllocatorPool heapAllocator;

  HiveORCZeroCopyShim(BufferAllocator allocator) {
    this.allocator = allocator;
    this.heapAllocator = new RecordReaderUtils.ByteBufferAllocatorPool();
  }

  @Override
  public void clear() {
    // Releasing any remaining direct buffers that were not released due to errors.
    for (ArrowBuf buf : directBufMap.values()) {
      buf.release();
    }
  }

  @Override
  public ByteBuffer getBuffer(boolean direct, int length) {
    if (!direct) {
      return heapAllocator.getBuffer(false, length);
    }
    ArrowBuf buf = allocator.buffer(length);
    ByteBuffer retBuf = buf.nioBuffer(0, length);
    directBufMap.put(new ByteBufferWrapper(retBuf), buf);
    return retBuf;
  }

  @Override
  public void putBuffer(ByteBuffer buffer) {
    if (!buffer.isDirect()) {
      heapAllocator.putBuffer(buffer);
      return;
    }
    ArrowBuf buf = directBufMap.remove(new ByteBufferWrapper(buffer));
    if (buf != null) {
      buf.release();
    }
  }
}
