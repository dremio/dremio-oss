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
package com.dremio.sabot.exec;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Allows for tracking of child allocators, such that when the last child allocator is closed, the allocator associated
 * with this TicketWithChildren can be closed too
 */
public class TicketWithChildren implements AutoCloseable {
  protected final BufferAllocator allocator;
  private final AtomicInteger childCount = new AtomicInteger();
  private volatile boolean closed;

  public TicketWithChildren(BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.closed = false;
  }

  public void reserve() {
    Preconditions.checkState(!closed, "Trying to reserve from a closed ticket");
    childCount.incrementAndGet();
  }

  public BufferAllocator getAllocator() {
    Preconditions.checkState(!closed, "Trying to access a closed ticket");
    return allocator;
  }

  public boolean release() {
    Preconditions.checkState(!closed, "Trying to release from a closed ticket");
    return childCount.decrementAndGet() == 0;
  }

  @Override
  public void close() throws Exception {
    Preconditions.checkState(!closed, "Trying to close an already closed ticket");
    closed = true;
    allocator.close();
  }

  @VisibleForTesting
  int getNumChildren() {
    return childCount.get();
  }
}
