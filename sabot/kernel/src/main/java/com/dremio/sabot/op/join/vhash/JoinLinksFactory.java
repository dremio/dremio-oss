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
package com.dremio.sabot.op.join.vhash;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.sabot.op.common.hashtable.HashTable;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;
import io.netty.util.internal.PlatformDependent;

/**
 * Creates {@link JoinLinks} objects whose underlying {@link io.netty.buffer.ArrowBuf}s are a given size
 */
class JoinLinksFactory {
  private final JoinLinksReusePool reusePool;
  private final BufferAllocator alloc;
  private final int numBufferRecords;
  private final int numBufferRecordsShift;   // == log2(numBufferRecords)
  private final int numBufferRecordsMask;

  /**
   * Create a factory that will produce {@link JoinLinks} objects.
   * Please note: the size of the underlying {@link ArrowBuf}s comes from the reusePool
   * @param reusePool   Pool to use if user requests a {@link JoinLinks} object with reused buffers
   */
  JoinLinksFactory(final JoinLinksReusePool reusePool, final BufferAllocator alloc) {
    this.reusePool = reusePool;
    this.alloc = alloc;
    this.numBufferRecords = reusePool.getNumBufferRecords();
    // numBufferRecords is a power of two
    Preconditions.checkArgument((numBufferRecords & (numBufferRecords - 1)) == 0);
    Preconditions.checkArgument(numBufferRecords > 0);
    this.numBufferRecordsShift = 31 - Integer.numberOfLeadingZeros(numBufferRecords);
    this.numBufferRecordsMask = numBufferRecords - 1;
  }

  /**
   * Create a new {@link JoinLinks}
   * The batch# for each of the records is initialized to JoinLinks.INDEX_EMPTY
   * @param numRecords  Number of records represented in the returned {@link JoinLinks}
   * @param isReuse     true, if the underlying {@link ArrowBuf}s should be reused buffers. False, if not
   * @return            the newly created {@link JoinLinks}
   */
  JoinLinks makeLinks(final int numRecords, final boolean isReuse) {
    if (numRecords <= numBufferRecords) {
      final ArrowBuf buf = allocateSingleBuf(isReuse);
      return new SingleBufJoinLinks(buf);
    }
    // multi-buffer
    final int numBuffers = (numRecords + (numBufferRecords - 1)) >>> numBufferRecordsShift;
    final ArrowBuf[] bufs = new ArrowBuf[numBuffers];
    for (int i = 0; i < numBuffers; i++) {
      bufs[i] = allocateSingleBuf(isReuse);
    }
    return new MultiBufJoinLinks(bufs);
  }

  private ArrowBuf allocateSingleBuf(final boolean isReuse) {
    final ArrowBuf buf = isReuse ? reusePool.getReusedBuf() : alloc.buffer(numBufferRecords * HashTable.BUILD_RECORD_LINK_SIZE);
    // NB: if allocation fails, underlying allocator would throw an exception. Getting control in the next line means buffer exists
    long addr = buf.memoryAddress();
    final long lastAddr = addr + numBufferRecords * HashTable.BUILD_RECORD_LINK_SIZE;
    for (; addr < lastAddr; addr += HashTable.BUILD_RECORD_LINK_SIZE) {
      PlatformDependent.putInt(addr, JoinLinks.INDEX_EMPTY);
    }
    return buf;
  }

  /**
   * Single-buffer version of JoinLinks
   */
  private class SingleBufJoinLinks implements JoinLinks {
    ArrowBuf linksBuf;

    SingleBufJoinLinks(final ArrowBuf linksBuf) {
      this.linksBuf = linksBuf;
    }

    public long linkMemoryAddress(final int recordNum) {
      return linksBuf.memoryAddress() + recordNum * HashTable.BUILD_RECORD_LINK_SIZE;
    }

    public void reuse() {
      reusePool.reuseBuf(linksBuf);
      linksBuf.release();
      linksBuf = null;
    }

    public void close() {
      if (linksBuf != null) {
        linksBuf.release();
      }
    }
  }

  /**
   * Multiple-buffer version of JoinLinks
   */
  private class MultiBufJoinLinks implements JoinLinks {
    final ArrowBuf[] linksBufs;

    MultiBufJoinLinks(final ArrowBuf[] linksBufs) {
      this.linksBufs = linksBufs;
    }

    public long linkMemoryAddress(final int recordNum) {
      return linksBufs[recordNum >>> numBufferRecordsShift].memoryAddress() +
        (recordNum & numBufferRecordsMask) * HashTable.BUILD_RECORD_LINK_SIZE;
    }

    public void reuse() {
      for (int i = 0; i < linksBufs.length; i++) {
        reusePool.reuseBuf(linksBufs[i]);
        linksBufs[i].release();
        linksBufs[i] = null;
      }
    }

    public void close() {
      for (final ArrowBuf buf : linksBufs) {
        if (buf != null) {
          buf.release();
        }
      }
    }
  }
}
