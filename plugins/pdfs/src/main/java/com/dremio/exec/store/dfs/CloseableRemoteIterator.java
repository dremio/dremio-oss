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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import org.apache.hadoop.fs.RemoteIterator;

/**
 * Remote iterator that wraps the given stream and supports {@link Closeable} for closing that stream.
 * <p>
 * Implicitly closes the wrapped stream on {@link #hasNext} check that returns {@code false}.
 *
 * @param <T> value type
 */
class CloseableRemoteIterator<T> implements RemoteIterator<T>, Closeable {
  private final Stream<T> stream;
  private final Iterator<T> iterator;

  private final AtomicBoolean closed = new AtomicBoolean(false);

  CloseableRemoteIterator(Stream<T> stream) {
    this.stream = stream;
    this.iterator = stream.iterator();
  }

  @Override
  public boolean hasNext() throws IOException {
    try {
      final boolean hasNext = iterator.hasNext();
      if (!hasNext) {
        close();
      }
      return hasNext;
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @Override
  public T next() throws IOException {
    try {
      return iterator.next();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @Override // idempotent
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      stream.close();
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  static <E> CloseableRemoteIterator<E> of(E singleton) {
    return new CloseableRemoteIterator<>(Stream.of(singleton));
  }
}
