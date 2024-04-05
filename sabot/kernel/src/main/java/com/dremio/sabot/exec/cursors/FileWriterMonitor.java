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

/** Monitor to block writer if it's gets too much ahead of the readers. */
class FileWriterMonitor {
  private static final int MAX_GAP = 3;
  private final SharedResource resource;
  private long writeCursor;
  private long maxReadCursor;

  FileWriterMonitor(SharedResource resource) {
    this.resource = resource;
    resource.markAvailable();
  }

  void updateWriteCursor(long updatedCursor) {
    synchronized (resource) {
      Preconditions.checkArgument(updatedCursor >= this.writeCursor);

      writeCursor = updatedCursor;
      if (writeCursor - maxReadCursor >= MAX_GAP) {
        // writer has moved too much ahead of the closest reader, block the writer resource.
        resource.markBlocked();
      }
    }
  }

  void updateMaxReadCursor(long updatedCursor) {
    synchronized (resource) {
      Preconditions.checkArgument(updatedCursor >= this.maxReadCursor);
      maxReadCursor = updatedCursor;
      if (!resource.isAvailable() && (writeCursor - maxReadCursor < MAX_GAP)) {
        // at least one reader has come close to the writer, unblock the writer resource.
        resource.markAvailable();
      }
    }
  }

  void wakeUp() {
    // wakeup writer, so that it can check if it still needs to continue.
    synchronized (resource) {
      resource.markAvailable();
    }
  }
}
