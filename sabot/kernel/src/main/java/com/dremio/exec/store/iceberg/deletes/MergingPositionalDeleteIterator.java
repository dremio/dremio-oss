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
package com.dremio.exec.store.iceberg.deletes;

import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

import com.dremio.common.AutoCloseables;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * A PositionalDeleteIterator that exposes positional deletes applicable to a single data file, that are the merged
 * from multiple SingleFilePositionalDeleteIterators.
 */
public class MergingPositionalDeleteIterator implements PositionalDeleteIterator {

  private final List<IteratorAndCurrentPos> deleteFileIterators;
  private final PriorityQueue<IteratorAndCurrentPos> priorityQueue;

  public MergingPositionalDeleteIterator(List<PositionalDeleteIterator> deleteFileIterators) {
    this.deleteFileIterators = deleteFileIterators.stream()
      .map(IteratorAndCurrentPos::new)
      .collect(Collectors.toList());
    this.priorityQueue = new PriorityQueue<>();
    // advanceAndEnqueue may modify the deleteFileIterators list, so make a temporary list to iterate over
    ImmutableList.copyOf(this.deleteFileIterators).forEach(this::advanceAndEnqueue);
  }

  public static PositionalDeleteIterator merge(List<PositionalDeleteIterator> deleteFileIterators) {
    Preconditions.checkArgument(deleteFileIterators.size() > 0);
    if (deleteFileIterators.size() == 1) {
      return deleteFileIterators.get(0);
    }

    return new MergingPositionalDeleteIterator(deleteFileIterators);
  }

  @Override
  public boolean hasNext() {
    return !priorityQueue.isEmpty();
  }

  @Override
  public Long next() {
    IteratorAndCurrentPos iteratorAndCurrentPos = priorityQueue.poll();
    long pos = iteratorAndCurrentPos.getPos();
    advanceAndEnqueue(iteratorAndCurrentPos);

    return pos;
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(deleteFileIterators);
  }

  private void closeIterator(IteratorAndCurrentPos iteratorAndCurrentPos) {
    try {
      iteratorAndCurrentPos.close();
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      deleteFileIterators.remove(iteratorAndCurrentPos);
    }
  }

  private void advanceAndEnqueue(IteratorAndCurrentPos iteratorAndCurrentPos) {
    if (iteratorAndCurrentPos.advance()) {
      priorityQueue.add(iteratorAndCurrentPos);
    } else {
      closeIterator(iteratorAndCurrentPos);
    }
  }

  private static class IteratorAndCurrentPos implements Comparable<IteratorAndCurrentPos>, AutoCloseable {
    private final PositionalDeleteIterator iterator;
    private long pos;

    public IteratorAndCurrentPos(PositionalDeleteIterator iterator) {
      Preconditions.checkArgument(!(iterator instanceof MergingPositionalDeleteIterator),
          "Nested MergingPositionalDeleteIterators is not supported.");
      this.iterator = iterator;
    }

    public long getPos() {
      return pos;
    }

    public PositionalDeleteIterator getIterator() {
      return iterator;
    }

    public boolean advance() {
      if (iterator.hasNext()) {
        pos = iterator.next();
        return true;
      }

      return false;
    }

    @Override
    public int compareTo(IteratorAndCurrentPos other) {
      return Long.compare(this.pos, other.pos);
    }

    @Override
    public void close() throws Exception {
      iterator.close();
    }
  }
}
