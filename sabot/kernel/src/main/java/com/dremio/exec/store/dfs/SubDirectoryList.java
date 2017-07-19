/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.hadoop.fs.FileStatus;

import com.dremio.exec.store.PartitionExplorer;

import java.util.Iterator;
import java.util.List;

public class SubDirectoryList implements Iterable<String>{
  final List<FileStatus> fileStatuses;

  SubDirectoryList(List<FileStatus> fileStatuses) {
    this.fileStatuses = fileStatuses;
  }

  @Override
  public Iterator<String> iterator() {
    return new SubDirectoryIterator(fileStatuses.iterator());
  }

  private class SubDirectoryIterator implements Iterator<String> {

    final Iterator<FileStatus> fileStatusIterator;

    SubDirectoryIterator(Iterator<FileStatus> fileStatusIterator) {
      this.fileStatusIterator = fileStatusIterator;
    }

    @Override
    public boolean hasNext() {
      return fileStatusIterator.hasNext();
    }

    @Override
    public String next() {
      return fileStatusIterator.next().getPath().toUri().toString();
    }

    /**
     * This class is designed specifically for use in conjunction with the
     * {@link com.dremio.exec.store.PartitionExplorer} interface.
     * This is only designed for accessing partition information, not
     * modifying it. To avoid confusing users of the interface this
     * method throws UnsupportedOperationException.
     *
     * @throws UnsupportedOperationException - this is not useful here, the
     *           list being iterated over should not be used in a way that
     *           removing an element would be meaningful.
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException(String.format("Cannot modify partition information through the " +
          "%s interface.", PartitionExplorer.class.getSimpleName()));
    }
  }
}
