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
package com.dremio.io.file;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.dremio.common.AutoCloseables;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

/**
 * Lazy file iterator that could exclude hidden files and folders
 * if a folder is treated as hidden, all its content is ignored
 */
class RecursiveDirectoryStream implements DirectoryStream<FileAttributes> {
  private final Stack<Map.Entry<DirectoryStream<FileAttributes>, Iterator<FileAttributes>>> iteratorsStack;
  private final FileSystem fileSystem;
  private final Predicate<Path> pathFilter;

  private final AtomicBoolean init = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);


  public RecursiveDirectoryStream(@Nonnull FileSystem fileSystem, @Nonnull DirectoryStream<FileAttributes> stream, Predicate<Path> pathFilter) throws IOException {
    this.pathFilter = pathFilter;
    this.fileSystem = fileSystem;
    iteratorsStack = new Stack<>();
    pushToStack(stream);
  }

  @Override
  public void close() throws IOException {
    if (!closed.compareAndSet(false, true)) {
      return;
    }

    try {
      AutoCloseables.close(Iterables.transform(iteratorsStack, Map.Entry::getKey));
    } catch (IOException | RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
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
      private FileAttributes lastFetchedElement = null;

      @Override
      public boolean hasNext() {
        // we have already prefetched a file and we know that it satisfies a path filter (this happens when hasNext
        // called several times without next call)
        if (lastFetchedElement != null) {
          return true;
        }

        // lastFetchedElement is empty here
        while (!iteratorsStack.empty()) {
          final Map.Entry<DirectoryStream<FileAttributes>, Iterator<FileAttributes>> currentEntry = iteratorsStack.peek();
          final Iterator<FileAttributes> currentIterator = currentEntry.getValue();
          if (!currentIterator.hasNext()) {
            try {
              iteratorsStack.pop();
              currentEntry.getKey().close();
            } catch (IOException e) {
              throw new DirectoryIteratorException(e);
            }
            continue;
          }

          // we need to prefetch the next element to check if its path satisfies the filter
          // also mandated by DirectoryStream interface that if hasNext() return true, next() should not throw.
          final FileAttributes currentFileAttributes = currentIterator.next();
          final Path currentPath = currentFileAttributes.getPath();
          if (pathFilter.test(currentPath)) {
            // a next element is found
            lastFetchedElement = currentFileAttributes;

            // add current folder to the iterator stack.  Will be used once the other iterators are exhausted.
            if (currentFileAttributes.isDirectory()) {
              try {
                pushToStack(fileSystem.list(currentPath, pathFilter));
              } catch (IOException e) {
                throw new DirectoryIteratorException(e);
              }
            }

            return true;
          }
        }

        // a next element is not found
        return false;
      }

      @Override
      public FileAttributes next() {
        if (this.hasNext()) {
          // hasNext populates {@code lastFetchedElement} field that we should use
          final FileAttributes fileAttributes = lastFetchedElement;
          //reset {@code lastFetchedElement} as we are going to return it
          lastFetchedElement = null;

          return fileAttributes;
        }

        throw new NoSuchElementException();
      }
    };
  }

  private void pushToStack(DirectoryStream<FileAttributes> stream) {
    iteratorsStack.push(new AbstractMap.SimpleEntry<>(stream, stream.iterator()));
  }

  @VisibleForTesting
  protected int getStackSize() {
    return iteratorsStack.size();
  }
}
