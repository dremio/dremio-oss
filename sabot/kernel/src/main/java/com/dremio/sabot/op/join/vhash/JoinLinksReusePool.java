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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.sabot.op.common.hashtable.HashTable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.netty.buffer.ArrowBuf;

/**
 * Allows for reuse of {@link ArrowBuf}s that store the links in the HashJoin's hash table. Intended for use
 * during detangling.
 * All buffers managed by the reuse pool have to be the same capacity, set at construction
 * This class is single-threaded
 */
public class JoinLinksReusePool implements AutoCloseable {
  private final int numBufferRecords;
  private final BufferAllocator alloc;
  private final List<ArrowBuf> bufs;

  /**
   * Create a new links reuse pool. The pool will be initialized with a few buffers of a given capacity
   * @param numBufferRecords   Size of the buffers managed by the reuse pool. Specified in the number of records (not bytes)
   *                           Must be a power of two
   * @param numInitialBufs     Number of buffers allocated by the reuse pool at construction
   * @param alloc              Allocator used to allocate the initial buffers
   */
  JoinLinksReusePool(final int numBufferRecords, final int numInitialBufs, final BufferAllocator alloc) {
    Preconditions.checkArgument((numBufferRecords & (numBufferRecords - 1)) == 0);
    this.numBufferRecords = numBufferRecords;
    this.alloc = alloc;
    this.bufs = new ArrayList<>();
    final int bufBytes = getBufferSizeBytes();
    for (int i = 0; i < numInitialBufs; i++) {
      bufs.add(alloc.buffer(bufBytes));
    }
  }

  /**
   * @return  The size of the buffers managed by the reuse pool. Specified as a number of records (not bytes)
   */
  int getNumBufferRecords() {
    return numBufferRecords;
  }

  private int getBufferSizeBytes() {
    return numBufferRecords * HashTable.BUILD_RECORD_LINK_SIZE;
  }

  @VisibleForTesting
  int getNumBuffers() {
    return bufs.size();
  }

  /**
   * Returns one of the reused {@link ArrowBuf}s. If there are no remaining {@link ArrowBuf}s, it will attempt
   * to allocate a new one
   * The returned {@link ArrowBuf} will already have a reference count on it -- similar to what happens
   * in {@link BufferAllocator#buffer(int)}
   */
  ArrowBuf getReusedBuf() {
    final int numBufs = bufs.size();
    if (numBufs == 0) {
      return alloc.buffer(getBufferSizeBytes());
    }
    final ArrowBuf buf = bufs.get(numBufs - 1);
    bufs.remove(numBufs - 1);
    return buf;
  }

  /**
   * Add a buffer to the pool of reused buffers. The reuse pool will take a reference to this {@link ArrowBuf}
   */
  void reuseBuf(final ArrowBuf buf) {
    buf.retain();
    bufs.add(buf);
  }

  /**
   * Reset the pool of reused buffers down to an initial value. No action will be taken if there are already
   * fewer than 'numBuffers' buffers
   * @param numBuffers Number of buffers to which to downsize this reuse pool
   */
  void reset(final int numBuffers) {
    while (bufs.size() > numBuffers) {
      bufs.get(bufs.size() - 1).release();
      bufs.remove(bufs.size() - 1);
    }
  }

  /**
   * Returns any buffers still in the pool back to the system
   */
  public void close() throws Exception {
    AutoCloseables.close(bufs);
  }
}
