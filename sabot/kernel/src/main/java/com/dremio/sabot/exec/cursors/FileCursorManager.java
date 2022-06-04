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
package com.dremio.sabot.exec.cursors;

import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.threads.sharedres.SharedResource;

/**
 * Interface for read/write cursor manager for a directory.
 */
public interface FileCursorManager {
  interface Observer extends AutoCloseable {
    /**
     * Update the cursor for this reader/writer.
     *
     * @param fileSeq file seq number
     * @param cursor monotonically increasing cursor
     */
    void updateCursor(int fileSeq, long cursor);
  };

  /**
   * Register a writer.
   *
   * @param streamManager manager for providing input/output streams
   * @param resource shared resource to block/unblock based on the cursor movement.
   * @param onAllReadersDone callback to be invoked when all readers are finished.
   * @return an observer to track cursor movement
   */
  Observer registerWriter(FileStreamManager streamManager, SharedResource resource, Runnable onAllReadersDone);

  /**
   * Register a reader.
   *
   * @param resource shared resource to block/unblock based on the cursor movement.
   * @return an observer to track cursor movement
   */
  Observer registerReader(SharedResource resource);

  /**
   * Notify that all registrations are now done.
   */
  void notifyAllRegistrationsDone();

  /**
   * Get the associated stream manager (can return null if there is no writer).
   * @return stream manager.
   */
  FileStreamManager getFileStreamManager();

  /**
   * Get the value of the current write cursor.
   * @return cursor value.
   */
  long getWriteCursor();

  /**
   * Get the max value of the all read cursors.
   * @return cursor value.
   */
  long getMaxReadCursor();
}
