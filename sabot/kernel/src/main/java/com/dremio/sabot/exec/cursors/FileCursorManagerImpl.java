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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.sabot.exec.rpc.FileStreamManager;
import com.dremio.sabot.threads.sharedres.SharedResource;
import com.google.common.base.Preconditions;

/**
 * Impl of cursor manager : tracks cursor for one writer and all readers.
 */
public class FileCursorManagerImpl implements FileCursorManager {
  private static final Logger logger = LoggerFactory.getLogger(FileCursorManagerImpl.class);
  private static final long INVALID_CURSOR = -1;
  private final String id;
  private FileStreamManager fileStreamManager;
  private FileWriterMonitor writerMonitor;
  private final List<FileReaderMonitor> allReaderMonitors = new ArrayList<>();
  private Runnable onAllReadersDone;
  private long curWriteCursor = INVALID_CURSOR;
  private long curMaxReadCursor = INVALID_CURSOR;
  private boolean allRegistrationsDone;

  FileCursorManagerImpl(String id) {
    this.id = id;
  }

  @Override
  public Observer registerWriter(FileStreamManager fileStreamManager, SharedResource resource, Runnable onAllReadersDone) {
    Preconditions.checkState(this.fileStreamManager == null, "writer already registered");
    Preconditions.checkState(!allRegistrationsDone);

    this.fileStreamManager = fileStreamManager;
    setWriterMonitor(new FileWriterMonitor(resource), onAllReadersDone);
    logger.debug("registered writer");
    return new Observer() {
      @Override
      public void updateCursor(int fileSeq, long cursor) {
        //logger.debug("updating write cursor to {}", cursor);
        updateWriteCursorAndNotifyReaders(cursor);
      }

      @Override
      public void close() throws Exception {
        clearWriterMonitor();
        checkAndCleanup();
        logger.debug("unregistered writer");
      }
    };
  }

  @Override
  public Observer registerReader(SharedResource resource) {
    Preconditions.checkState(!allRegistrationsDone);
    final FileReaderMonitor readerMonitor = new FileReaderMonitor(resource);
    logger.debug("registered reader");

    addReaderMonitor(readerMonitor);
    return new Observer() {
      @Override
      public void updateCursor(int fileSeq, long cursor) {
        //logger.debug("updating read cursor to {}", cursor);
        updateReadCursorAndNotifyWriter(readerMonitor, cursor);
        // TODO: use the fileSeq to delete no-longer-needed files.
      }

      @Override
      public void close() throws Exception {
        removeReaderMonitor(readerMonitor);
        checkAndCleanup();
        logger.debug("unregistered reader");
      }
    };
  }

  @Override
  public void notifyAllRegistrationsDone() {
    logger.debug("all registrations done");
    synchronized (this) {
      allRegistrationsDone = true;
      if (writerMonitor != null) {
        writerMonitor.wakeUp();
      }
    }
    checkAndFireWriterCallback();
    try {
      checkAndCleanup();
    } catch (Exception ignore) {}
  }

  @Override
  public FileStreamManager getFileStreamManager() {
    return fileStreamManager;
  }

  @Override
  public long getWriteCursor() {
    return curWriteCursor;
  }

  @Override
  public long getMaxReadCursor() {
    return curMaxReadCursor;
  }

  public String getId() {
    return id;
  }

  private synchronized void setWriterMonitor(FileWriterMonitor writerMonitor, Runnable onAllReadersDone) {
    this.writerMonitor = writerMonitor;
    this.onAllReadersDone = onAllReadersDone;
  }

  private synchronized void clearWriterMonitor() {
    this.writerMonitor = null;
    this.onAllReadersDone = null;
    for (FileReaderMonitor readerMonitor : allReaderMonitors) {
      readerMonitor.markWriterFinished();
    }
  }

  private synchronized void updateWriteCursorAndNotifyReaders(long cursor) {
    curWriteCursor = cursor;
    writerMonitor.updateWriteCursor(cursor);

    // notify all registered readers.
    for (FileReaderMonitor readerMonitor : allReaderMonitors) {
      readerMonitor.updateWriteCursor(cursor);
    }
  }

  private synchronized void addReaderMonitor(FileReaderMonitor readerMonitor) {
    allReaderMonitors.add(readerMonitor);
    if (writerMonitor == null && curWriteCursor != INVALID_CURSOR) {
      // writer finished even before reader registered.
      readerMonitor.markWriterFinished();
    }
    if (curWriteCursor != INVALID_CURSOR) {
      readerMonitor.updateWriteCursor(curWriteCursor);
    }
  }

  private void removeReaderMonitor(FileReaderMonitor readerMonitor) {
    synchronized (this) {
      allReaderMonitors.remove(readerMonitor);
    }
    checkAndFireWriterCallback();
  }

  private void checkAndFireWriterCallback() {
    Runnable callback = null;
    synchronized (this) {
      if (allRegistrationsDone && allReaderMonitors.isEmpty() && onAllReadersDone != null) {
        // notify writer that there are no more readers.
        callback = onAllReadersDone;
      }
    }
    if (callback != null) {
      callback.run();
    }
  }

  private synchronized void updateReadCursorAndNotifyWriter(FileReaderMonitor readerMonitor, long cursor) {
    Preconditions.checkArgument(cursor <= curWriteCursor,
      "reader cannot be ahead of writer, read cursor " + cursor + " write cursor " + curWriteCursor);
    readerMonitor.updateReadCursor(cursor);

    if (cursor > curMaxReadCursor) {
      curMaxReadCursor = cursor;
      if (writerMonitor != null) {
        writerMonitor.updateMaxReadCursor(cursor);
      }
    }
  }

  private synchronized void checkAndCleanup() throws IOException {
    if (allRegistrationsDone && writerMonitor == null && allReaderMonitors.isEmpty() && fileStreamManager != null) {
      fileStreamManager.deleteAll();
    }
  }
}
