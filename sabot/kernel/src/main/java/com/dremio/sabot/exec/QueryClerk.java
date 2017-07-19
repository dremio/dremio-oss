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
package com.dremio.sabot.exec;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.UserBitShared.QueryId;
import com.google.common.base.Preconditions;

/**
 * Manages the query level allocator and potentially the query scheduling group.<br>
 * Each query fragment should cause {@link #reserve()} to be called once, no matter how many child allocators the fragment
 * ends up allocating from the query allocator.<br>
 * Fragments are also expected to close the query allocator exactly once after closing all child allocators. Once the
 * the last known reservation is closed the query allocator closes its corresponding buffer allocator, any further
 * operation on the query allocator will throw an {@link IllegalStateException}
 */
class QueryClerk implements AutoCloseable {
  private final QueryId queryId;
  private final BufferAllocator allocator;
  private final AtomicInteger childCount = new AtomicInteger();
  private volatile boolean closed;

  public QueryClerk(QueryId queryId, BufferAllocator allocator) {
    this.allocator = Preconditions.checkNotNull(allocator, "allocator cannot be null");
    this.queryId = Preconditions.checkNotNull(queryId, "queryId cannot be null");
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public void reserve() {
    Preconditions.checkState(!closed, "Trying to reserve a closed query clerk");
    childCount.incrementAndGet();
  }

  public BufferAllocator getAllocator() {
    Preconditions.checkState(!closed, "Trying to access a closed query clerk");
    return allocator;
  }

  public boolean release() {
    Preconditions.checkState(!closed, "Query clerk already closed");
    return childCount.decrementAndGet() == 0;
  }

  @Override
  public void close() throws Exception {
    Preconditions.checkState(!closed, "Query clerk already closed");
    closed = true;
    allocator.close();
  }
}
