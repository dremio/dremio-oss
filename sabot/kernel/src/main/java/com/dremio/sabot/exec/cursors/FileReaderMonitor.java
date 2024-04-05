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

import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Monitor to block reader if it has caught up with the writer. */
class FileReaderMonitor {
  private static final Logger logger = LoggerFactory.getLogger(FileReaderMonitor.class);
  private final SharedResource resource;
  private long writeCursor;
  private long readCursor;
  private boolean writerFinished;

  FileReaderMonitor(SharedResource resource) {
    this.resource = resource;
    resource.markBlocked();
  }

  void updateWriteCursor(long updatedCursor) {
    synchronized (resource) {
      Preconditions.checkArgument(updatedCursor >= writeCursor);

      writeCursor = updatedCursor;
      if (!resource.isAvailable() && writeCursor > readCursor) {
        // writer has moved ahead, unblock the reader resource.
        // logger.debug("unblock resource {} writeCursor {} readCursor {}", resource.getName(),
        // writeCursor, readCursor);
        resource.markAvailable();
      }
    }
  }

  void markWriterFinished() {
    synchronized (resource) {
      writerFinished = true;
      resource.markAvailable();
    }
  }

  void updateReadCursor(long updatedCursor) {
    synchronized (resource) {
      Preconditions.checkArgument(
          updatedCursor >= readCursor,
          "cursor should not decrease, updatedCursor "
              + updatedCursor
              + " readCursor "
              + readCursor);
      Preconditions.checkArgument(
          updatedCursor <= writeCursor,
          "read cursor should be <= writeCursor, updatedCursor "
              + updatedCursor
              + " writeCursor "
              + writeCursor);
      readCursor = updatedCursor;
      if (readCursor == writeCursor && !writerFinished) {
        // reader has caught up to the writer, block the reader resource.
        // logger.debug("block resource {} writeCursor {} readCursor {}", resource.getName(),
        // writeCursor, readCursor);
        resource.markBlocked();
      }
    }
  }

  long getReadCursor() {
    return readCursor;
  }

  long getWriteCursor() {
    return writeCursor;
  }
}
