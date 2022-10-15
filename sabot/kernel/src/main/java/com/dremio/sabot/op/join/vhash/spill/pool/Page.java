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

import org.apache.arrow.memory.ArrowBuf;

/**
 * Provides a piece of memory of a certain size that can be sub-divided. Memory
 * is only exposed by asking for slices. Page will keep track of how much memory
 * has been sliced to report occupancy. Pages can only be sliced once but the
 * underlying page can be reused via the PagePool.
 */
public interface Page extends AutoCloseable {
  /**
   * Get the size of the page in bytes.
   * @return page size
   */
  int getPageSize();

  /**
   * Get the start address of the page.
   * @return start address.
   */
  long getAddress();

  /**
   * Get the backing buffer for the page.
   * @return arrow buf.
   */
  ArrowBuf getBackingBuf();

  /**
   * Get a slice of memory from this page. This is a reference counted object
   * incremented each time you call this method.
   *
   * @param size
   *          The size of the desired memory.
   * @return The buffer that holds this memory, with a reference.
   */
  ArrowBuf slice(int size);

  /**
   * Get a slice of memory from this page, aligned at 8-byte boundary. This is a reference counted object
   * incremented each time you call this method.
   *
   * @param size
   *          The size of the desired memory.
   * @return The buffer that holds this memory, with a reference.
   */
  ArrowBuf sliceAligned(int size);

  /**
   * Use up memory for purposes of accounting. Similar to a slice, the corresponding
   * size is marked as used/unavailable. No object needs to be returned/managed.
   *
   * @param size
   *          The size of the memory to use up.
   */
  void deadSlice(int size);

  /**
   * Bytes in the page that are yet unused.
   * @return count of unused bytes
   */
  int getRemainingBytes();

  /**
   * Take a ref on the page.
   */
  void retain();

  /**
   * Recycle the page back to the pool for future use. This does not release any
   * memory back to the system, only the pool.
   */
  void release();
}
