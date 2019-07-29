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
package com.dremio.common.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationOutcome;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocator;

import com.dremio.common.exceptions.UserException;

import io.netty.buffer.ArrowBuf;

/**
 * The root allocator for using direct memory inside a Dremio process.
 * Tracks all top-level allocators
 */
public class DremioRootAllocator extends RootAllocator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioRootAllocator.class);

  private final ConcurrentMap<String, BufferAllocator> children;

  public static DremioRootAllocator create(final long limit, long maxBufferCount) {
    RootAllocatorListener listener = new RootAllocatorListener(maxBufferCount);
    DremioRootAllocator rootAllocator = new DremioRootAllocator(listener, limit);
    listener.setRootAllocator(rootAllocator);
    return rootAllocator;
  }

  public long getAvailableBuffers() {
    return listener.getAvailableBuffers();
  }
  /**
   * Constructor, hidden from public use. Use {@link #create(long)} instead
   */

  private RootAllocatorListener listener;

  private DremioRootAllocator(final RootAllocatorListener listener, final long limit) {
    super(listener, limit);
    children = new ConcurrentHashMap<>();
    this.listener = listener;
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize) {
    throw new UnsupportedOperationException("Dremio's root allocator should not be used for direct allocations");
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize, BufferManager manager) {
    throw new UnsupportedOperationException("Dremio's root allocator should not be used for direct allocations");
  }

  private static class RootAllocatorListener implements AllocationListener {

    /*
     * Count of available buffers. Since this is optimistically changed, it can go be negative but it would be wrong for it stay negative for an
     * extended period of time or be hugely negative.
     */
    private final AtomicLong availBuffers;

    DremioRootAllocator rootAllocator;

    public RootAllocatorListener(long maxCount) {
      availBuffers = new AtomicLong(maxCount);
    }

    void setRootAllocator(DremioRootAllocator rootAllocator) {
      this.rootAllocator = rootAllocator;
    }

    public long getAvailableBuffers() {
      return availBuffers.get();
    }

    @Override
    public void onPreAllocation(long size) {
      // We don't decrement here since it means we'll possibly leak a reference.
      // However, we throw here since we need to throw before any partial-accounting occurs.
      if(availBuffers.get() < 1) {
        throw new OutOfMemoryException("Buffer count exceeds maximum.");
      }
    }

    @Override
    public void onAllocation(long size) {
      availBuffers.decrementAndGet();
    }

    @Override
    public void onRelease(long size) {
      availBuffers.incrementAndGet();
    }

    @Override
    public boolean onFailedAllocation(long size, AllocationOutcome outcome) {
      return false;
    }

    @Override
    public void onChildAdded(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
      if (parentAllocator == rootAllocator) { // Note: Intentional reference equality
        rootAllocator.children.put(childAllocator.getName(), childAllocator);
      }
    }

    @Override
    public void onChildRemoved(BufferAllocator parentAllocator, BufferAllocator childAllocator) {
      if (parentAllocator == rootAllocator) { // Note: Intentional reference equality
        rootAllocator.children.remove(childAllocator.getName(), childAllocator);
      }
    }
  }
}
