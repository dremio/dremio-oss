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

/**
 * A batch-full of links, each HashTable.BUILD_RECORD_LINK_SIZE bytes in size
 * - First 4 bytes are used to identify the batch and remaining 2 bytes for record within the batch.
 * Objects of this interface support both reuse and close semantics:
 * - on reuse, the internal buffer(s) used to represent the links are returned to a reuse pool
 * - on close, the internal buffer(s) used to represent the links are released back to the system
 */
public interface JoinLinks extends AutoCloseable {
  public static final int INDEX_EMPTY = -1;

  /**
   * @param recordNum Record whose link we're looking for
   * @return          The address of a HashTable.BUILD_RECORD_LINK_SIZE-byte location of the link for the given record
   */
  long linkMemoryAddress(int recordNum);

  /**
   * Give the internal buffer(s) used to represent the links to a pool from where they could be reused.
   * The contents of this object are now empty, and any further attempt to access a link's memory address will result
   * in an exception.
   * After this call {@link #close()} turns into a no-op
   */
  void reuse();
}
