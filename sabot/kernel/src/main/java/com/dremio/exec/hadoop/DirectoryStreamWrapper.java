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
package com.dremio.exec.hadoop;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.dremio.io.file.FileAttributes;

/**
 * Wrapper around Hadoop {@code org.apache.hadoop.fs.RemoteIterator}
 * @param <T>
 */
class DirectoryStreamWrapper implements DirectoryStream<FileAttributes> {
  private final RemoteIterator<FileStatus> remoteIterator;
  private final AtomicBoolean init = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public DirectoryStreamWrapper(RemoteIterator<FileStatus> remoteIterator) {
    this.remoteIterator = remoteIterator;
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    if (remoteIterator instanceof Closeable) {
      ((Closeable) remoteIterator).close();
    }
  }

  @Override
  public Iterator<FileAttributes> iterator() {
    if (!init.compareAndSet(false, true)) {
      throw new IllegalStateException("Iterator already accessed.");
    }

    if (!closed.compareAndSet(false, true)) {
      throw new IllegalStateException("Directory stream already closed.");
    }

    return new Iterator<FileAttributes>() {
      private FileAttributes current = null;

      @Override
      public boolean hasNext() {
        if (current != null) {
          return true;
        }

        try {
          if (!remoteIterator.hasNext()) {
            return false;
          }

          // DirectoryStream guarantees that next() will not throw an DirectoryIteratorException
          // if hasNext() was called first
          current = new FileStatusWrapper(remoteIterator.next());
          return true;
        } catch (IOException e) {
          throw new DirectoryIteratorException(e);
        }
      }

      @Override
      public FileAttributes next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        FileAttributes result = current;
        current = null;
        return result;
      }
    };
  }


}
