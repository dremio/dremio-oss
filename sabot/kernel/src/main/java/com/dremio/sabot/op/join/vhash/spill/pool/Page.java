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
package com.dremio.sabot.op.join.vhash.spill.pool;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.arrow.memory.ArrowBuf;

/**
 * Provides a piece of memory of a certain size that can be sub-divided. Memory
 * is only exposed by asking for slices. Page will keep track of how much memory
 * has been sliced to report occupancy. Pages can only be sliced once but the
 * underlying page can be reused via the PagePool.
 */
@NotThreadSafe
public class Page implements AutoCloseable {

  private final int pageSize;
  private final PagePool.Release release;
  private final ArrowBuf memory;
  private int offset = 0;
  private int referenceCount = 0;

  Page(int pageSize, ArrowBuf memory, PagePool.Release release) {
    this.pageSize = pageSize;
    this.memory = memory;
    this.release = release;
  }

  public int getPageSize() {
    checkHasReferences();
    return pageSize;
  }

  public long getAddress() {
    checkHasReferences();
    return memory.memoryAddress();
  }

  /**
   * Get a slice of memory from this page. This is a reference counted object
   * incremented each time you call this method.
   *
   * @param size
   *          The size of the desired memory.
   * @return The buffer that holds this memory, with a reference.
   */
  public ArrowBuf slice(int size) {
    checkHasReferences();
    if (size + offset > pageSize) {
      throw new IllegalArgumentException(String.format("Attempting to slice beyond limit. Desired size: %d, available space: %d.", size, pageSize - offset));
    }
    final ArrowBuf buf = memory.slice(offset, size);
    memory.getReferenceManager().retain();
    offset += size;
    return buf;
  }

  /**
   * Use up memory for purposes of accounting. Similar to a slice, the corresponding
   * size is marked as used/unavailable. No object needs to be returned/managed.
   *
   * @param size
   *          The size of the memory to use up.
   */
  public void deadSlice(int size) {
    checkHasReferences();
    slice(size).close();
  }

  public int getRemainingBytes() {
    checkHasReferences();
    return pageSize - offset;
  }

  private void checkHasReferences() {
    if (referenceCount == 0) {
      throw new IllegalStateException("Operation not allowed after page was closed.");
    }
  }

  private void checkNoReferences() {
    if (referenceCount > 0) {
      throw new IllegalStateException("Operation not allowed until page is closed.");
    }
    if (memory.refCnt() != 1) {
      throw new IllegalStateException("Unexpected refCnt on page buffer, expected 1 and found " + memory.refCnt());
    }
  }

  /**
   * Release the memory of this page. Should only be done if this page has no pending references.
   */
  void deallocate() {
    checkNoReferences();
    memory.close();
  }

  /**
   * Generate a new version of this page to slice. Can only be done once this page
   * has been closed.
   *
   * @return The new page pointing to the same memory as this page.
   */
  Page toNewPage() {
    checkNoReferences();
    return new Page(pageSize, memory, release);
  }

  void initialRetain() {
    checkNoReferences();
    referenceCount++;
  }

  public void retain() {
    checkHasReferences();
    referenceCount++;
  }

  /**
   * Recycle the page back to the pool for future use. This does not release any
   * memory back to the system, only the pool.
   */
  public void release() {
    checkHasReferences();

    referenceCount--;
    if (referenceCount == 0) {
      release.release(this);
    }
  }

  /**
   * Recycle the page back to the pool for future use. This does not release any
   * memory back to the system, only the pool.
   */
  @Override
  public void close() {
    release();
  }
}
