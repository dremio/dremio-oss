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
package com.dremio.exec.store.dfs;

import java.io.IOException;

/**
 * This interface represents an event record handler for processing and committing history
 * information of type T. Implementations of this interface should define the logic for processing
 * the history event information and committing it after processing.
 *
 * @param <T> The type of history information that this history event handler can handle, which must
 *     extend the {@link FileLoadInfo} interface.
 */
public interface HistoryEventHandler<T extends FileLoadInfo> extends AutoCloseable {

  /**
   * Commit the processed history event information. Implementations should define the logic for
   * committing the processed history event information after processing.
   *
   * @throws IOException If an I/O error occurs during the commit operation.
   */
  void commit() throws Exception;

  /**
   * Process the given file load information. Implementations should define the logic for processing
   * the file load history event information.
   *
   * @param fileLoadInfo The file load information of type T to be processed.
   * @throws IOException If an I/O error occurs during the process operation.
   */
  void process(T fileLoadInfo) throws Exception;

  /**
   * Entries made through snapshots added by job with jobId are reverted from table.
   *
   * @param jobId Job ID to be reverted.
   * @param ex The exception due to which tables have to be reverted.
   */
  void revert(String jobId, Exception ex);
}
