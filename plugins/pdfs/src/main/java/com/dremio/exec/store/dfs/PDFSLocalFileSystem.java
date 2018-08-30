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
package com.dremio.exec.store.dfs;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.RemoteIterator;

/**
 * A {@link RawLocalFileSystem} subclass supporting {@code org.apache.hadoop.fs.FileSystem#listStatusIterator(Path)}
 * method
 */
public final class PDFSLocalFileSystem extends RawLocalFileSystem {
  private static final class CloseableRemoteIterator<T> implements RemoteIterator<T>, Closeable {
    private final Stream<T> stream;
    private final Iterator<T> iterator;

    private CloseableRemoteIterator(Stream<T> stream) {
      this.stream = stream;
      this.iterator = stream.iterator();
    }

    private static <E> CloseableRemoteIterator<E> of(E singleton) {
      return new CloseableRemoteIterator<>(Stream.of(singleton));
    }

    @Override
    public boolean hasNext() throws IOException {
      try {
        return iterator.hasNext();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    };

    @Override
    public T next() throws IOException {
      try {
        return iterator.next();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }


    @Override
    public void close() throws IOException {
      try {
        stream.close();
      } catch (UncheckedIOException e) {
        throw e.getCause();
      }
    }
  }
  @Override
  public RemoteIterator<FileStatus> listStatusIterator(Path f) throws FileNotFoundException, IOException {
    File localf = pathToFile(f);
    java.nio.file.Path localp = localf.toPath();

    if (!localf.exists()) {
      throw new FileNotFoundException("File " + f + " does not exist");
    }

    if (localf.isFile()) {
      return CloseableRemoteIterator.of(getFileStatus(f));
    }

    final Stream<FileStatus> statuses = Files.list(localp)
        .map(p -> {
          Path hp = new Path(f, new Path(null, null, p.toFile().getName()));
          try {
            return getFileStatus(hp);
          } catch(IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    return new CloseableRemoteIterator<>(statuses);
  }
}
