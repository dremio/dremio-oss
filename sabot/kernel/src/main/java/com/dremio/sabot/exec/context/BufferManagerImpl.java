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
package com.dremio.sabot.exec.context;


import org.apache.arrow.memory.BufferAllocator;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.predicates.LongObjectPredicate;

import io.netty.buffer.ArrowBuf;

public class BufferManagerImpl implements SlicedBufferManager {
  private LongObjectHashMap<ArrowBuf> managedBuffers = new LongObjectHashMap<>();
  private final BufferAllocator allocator;

  /**
   * This class keeps a pre-allocated large buffer & serve any subsequent allocation
   * requests via slicing from this large buffer.
   * This helps in reducing the heap allocation overhead as sliced ArrowBuf has smaller footprint.
   * Also, the allocation of large buffer may be optimized by doing power-of-2 allocations.
   *
   * largeBufCapacity: the capacity of the largeBuf
   * largeBufUsed tracks: how much memory has been sliced away from the largeBuf
   */
  private int largeBufCapacity;
  private int largeBufUsed;
  private ArrowBuf largeBuf;


  public BufferManagerImpl(BufferAllocator allocator) {
    this.allocator = allocator;
  }

  public BufferManagerImpl(BufferAllocator allocator, int largeBufCapacity) {
    this.allocator = allocator;
    this.largeBufCapacity = largeBufCapacity;
    this.largeBuf = null;
    this.largeBufUsed = 0;
  }

  @Override
  public void close() {
    managedBuffers.forEach(new LongObjectPredicate<ArrowBuf>() {
      @Override
      public boolean apply(long key, ArrowBuf value) {
        value.release();
        return true;
      }
    });
    managedBuffers.clear();
  }

  public ArrowBuf replace(ArrowBuf old, long newSize) {
    if (managedBuffers.remove(old.memoryAddress()) == null) {
      throw new IllegalStateException("Tried to remove unmanaged buffer.");
    }
    old.release(1);
    return getManagedBuffer(newSize);
  }

  public ArrowBuf getManagedBuffer() {
    return getManagedBuffer(256);
  }

  public ArrowBuf getManagedBuffer(long size) {
    ArrowBuf newBuf = allocator.buffer(size, this);
    managedBuffers.put(newBuf.memoryAddress(), newBuf);
    return newBuf;
  }

  public ArrowBuf getManagedBufferSliced(long size) {

    if (size >= largeBufCapacity) {
      return getManagedBuffer(size);
    }

    final int availableSpace = (largeBufCapacity - largeBufUsed);

    if (size > availableSpace || largeBuf == null) {
      largeBuf = allocator.buffer(largeBufCapacity, this);
      managedBuffers.put(largeBuf.memoryAddress(), largeBuf);
      largeBufUsed = 0;
    }

    final ArrowBuf buf = largeBuf.slice(largeBufUsed, size);
    largeBufUsed += size;
    return buf;
  }
}
