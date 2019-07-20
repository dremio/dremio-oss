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

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Stack;

import javax.validation.constraints.NotNull;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.annotations.VisibleForTesting;

/**
 * Lazy file iterator that could exclude hidden files and folders
 * if a folder is treated as hidden, all it's content is ignored
 */
public class FileRecursiveIterator implements RemoteIterator<FileStatus> {
  private final static PathFilter DEFAULT_PATH_FILTER = path -> true;

  private final Stack<RemoteIterator<FileStatus>> iteratorsStack;
  private final FileSystemWrapper fileSystem;
  private final PathFilter pathFilter;

  private FileStatus lastFetchedElement = null;

  public FileRecursiveIterator(@NotNull FileSystemWrapper fileSystem, @NotNull Path rootPath, boolean includeHiddenFiles) throws IOException {
    pathFilter = includeHiddenFiles ? DEFAULT_PATH_FILTER : DefaultPathFilter.INSTANCE;
    this.fileSystem = fileSystem;
    iteratorsStack = new Stack<>();
    addIteratorForPath(rootPath);
  }

  /**
   * Checks whether a next element exists. If it exists, the private {@code lastFetchedElement} field must be not empty
   * @return
   * @throws IOException
   */
  @Override
  public boolean hasNext() throws IOException {
    // we have already prefetched a file and we know that it satisfies a path filter (this happens when hasNext
    // called several times without next call)
    if (lastFetchedElement != null) {
      return true;
    }
    // lastFetchedElement is empty here
    while (!iteratorsStack.empty()) {
      if (!iteratorsStack.peek().hasNext()) {
        iteratorsStack.pop();
        continue;
      }
      // we need prefetch a next element to check if it's path satisfies a filter
      final FileStatus currentFileStatus = iteratorsStack.peek().next();
      final Path currentPath = currentFileStatus.getPath();
      if (pathFilter.accept(currentPath)) {
        // a next element is found
        lastFetchedElement = currentFileStatus;

        // add current folder to the iterator stack.  Will be used once the other iterators are exhausted.
        if (currentFileStatus.isDirectory()) {
          addIteratorForPath(currentPath);
        }

        return true;
      }
    }

    // a next element is not found
    return false;
  }

  @Override
  public FileStatus next() throws IOException {
    if (this.hasNext()) {
      // hasNext populates {@code lastFetchedElement} field that we should use
      final FileStatus status = lastFetchedElement;
      //reset {@code lastFetchedElement} as we are going to return it
      lastFetchedElement = null;

      return status;
    }

    throw new NoSuchElementException();
  }

  private void addIteratorForPath(Path pathToAdd) throws IOException {
    iteratorsStack.push(fileSystem.listStatusIterator(pathToAdd));
  }

  @VisibleForTesting
  protected int getStackSize() {
    return iteratorsStack.size();
  }
}
