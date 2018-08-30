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
package com.dremio.common.memory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.BaseAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.apache.arrow.memory.RootAllocator;

import com.dremio.common.exceptions.UserException;

import io.netty.buffer.ArrowBuf;

/**
 * The root allocator for using direct memory inside a Dremio process.
 * Tracks all top-level allocators
 */
public class DremioRootAllocator extends RootAllocator {
  private final ConcurrentMap<String, BufferAllocator> children;

  public DremioRootAllocator(final long limit) {
    super(limit);
    children = new ConcurrentHashMap<>();
  }

  @Override
  public BufferAllocator newChildAllocator(final String name, final AllocationListener listener,
                                           final long initReservation, final long maxAllocation) {
    //TODO: after ARROW-2165 is implemented, replace override of newChildAllocator() with a AllocatorListener listening
    //TODO: for additions and removals of children
    BufferAllocator childAllocator = super.newChildAllocator(name, listener, initReservation, maxAllocation);
    children.put(name, childAllocator);
    return childAllocator;
  }

  /**
   * Add the memory usage of the root allocator and all of its children to an exception
   */
  public void addUsageToExceptionContext(UserException.Builder b) {
    // NB: allocator name already printed in each allocator's toString()
    b.addContext(toString().trim());
    // in DEBUG mode, children are already printed as part of the allocator's toString()
    if (!BaseAllocator.isDebug()) {
      for (String childAllocatorName : children.keySet()) {
        b.addContext("  ", children.get(childAllocatorName).toString().trim());
      }
    }
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize) {
    throw new UnsupportedOperationException("Dremio's root allocator should not be used for direct allocations");
  }

  @Override
  public ArrowBuf buffer(final int initialRequestSize, BufferManager manager) {
    throw new UnsupportedOperationException("Dremio's root allocator should not be used for direct allocations");
  }
}
