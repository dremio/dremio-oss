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
package com.dremio.exec.util;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.collect.Iterators;

/**
 * Utilities to work with Hadoop's RemoteIterator.
 */
public class RemoteIterators {

  /**
   * Filter a RemoteIterator based on a predicate that is allowed to throw an IOException.
   *
   * @param iter The RemoteIterator to filter.
   * @param predicate The predicate to apply.
   * @return the new RemoteIterator
   */
  public static RemoteIterator<LocatedFileStatus> filter(RemoteIterator<LocatedFileStatus> iter, PredicateWithIOException<LocatedFileStatus> predicate) {
    return new RemoteIterators.IterToRemote(Iterators.filter(
        new RemoteIterators.RemoteToIter(iter),
        t -> {
          try {
            return predicate.apply(t);
          } catch (IOException ex) {
            throw new CaughtIO(ex);
          }
        }
    ));
  }

  /**
   * Transform a RemoteIteartor based on a transformation function that is allowed to throw an IOException.
   * @param iter The RemoteIterator to transform
   * @param func The function to use for transforming the values.
   * @return The new RemoteIterator.
   */
  public static RemoteIterator<LocatedFileStatus> transform(RemoteIterator<LocatedFileStatus> iter, final FunctionWithIOException<LocatedFileStatus, LocatedFileStatus> func) {
    return new RemoteIterators.IterToRemote(Iterators.transform(
        new RemoteIterators.RemoteToIter(iter),
        t -> {
          try {
            return func.apply(t);
          } catch (IOException ex) {
            throw new CaughtIO(ex);
          }
        }));
  }

  public static interface PredicateWithIOException<IN> {
    boolean apply(IN input) throws IOException;
  }

  public static interface FunctionWithIOException<IN, OUT> {
    OUT apply(IN in) throws IOException;
  }

  /**
   * Marker exception used to propagate a runtime exception across traditional boundaries.
   */
  private static class CaughtIO extends RuntimeException {

    public CaughtIO(IOException cause) {
      super(cause);
    }
  }

  /**
   * Converts a Iterator into a RemoteIterator, revealing any CaughtIO exception.
   */
  private static class IterToRemote implements RemoteIterator<LocatedFileStatus> {

    private final Iterator<LocatedFileStatus> delegate;

    public IterToRemote(Iterator<LocatedFileStatus> delegate) {
      super();
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() throws IOException {
      try {
        return delegate.hasNext();
      }catch (CaughtIO caught) {
        throw (IOException) caught.getCause();
      }
    }

    @Override
    public LocatedFileStatus next() throws IOException {
      try {
        return delegate.next();
      }catch (CaughtIO caught) {
        throw (IOException) caught.getCause();
      }
    }

  }

  /**
   * Converts a RemoteIterator into a normal java.util.Iterator, cloaking IOExceptions.
   */
  private static class RemoteToIter implements Iterator<LocatedFileStatus> {

    private final RemoteIterator<LocatedFileStatus> delegate;

    public RemoteToIter(RemoteIterator<LocatedFileStatus> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      try {
        return delegate.hasNext();
      }catch (IOException caught) {
        throw new CaughtIO(caught);
      }
    }

    @Override
    public LocatedFileStatus next() {
      try {
        return delegate.next();
      }catch (IOException caught) {
        throw new CaughtIO(caught);
      }
    }

  }
}
