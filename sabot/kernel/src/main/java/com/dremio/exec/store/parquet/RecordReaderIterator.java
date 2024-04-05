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

package com.dremio.exec.store.parquet;

import com.dremio.common.AutoCloseables;
import com.dremio.common.util.CloseableIterator;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.SplitAndPartitionInfo;
import com.dremio.sabot.exec.store.parquet.proto.ParquetProtobuf;
import com.google.common.collect.Iterators;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Split iterator for the scan */
public interface RecordReaderIterator extends CloseableIterator<RecordReader> {

  /**
   * Adds runtime filter to the iterator, to ensure unnecessary splits are not under iteration
   * result.
   *
   * @param runtimeFilter
   */
  void addRuntimeFilter(RuntimeFilter runtimeFilter);

  /**
   * Return all the runtime filters added to the iterator. Might be required to add these filters to
   * the next iterator
   *
   * @return
   */
  List<RuntimeFilter> getRuntimeFilters();

  default ParquetProtobuf.ParquetDatasetSplitScanXAttr getCurrentSplitXAttr() {
    return null;
  }

  default SplitAndPartitionInfo getCurrentSplitAndPartitionInfo() {
    return null;
  }

  /**
   * Mark the iterator to start producing from buffered readers
   *
   * @param toProduce
   */
  void produceFromBuffered(boolean toProduce);

  /**
   * Returns singleton iterator
   *
   * @param recordReader
   * @return
   */
  static RecordReaderIterator from(RecordReader recordReader) {
    return from(Iterators.singletonIterator(recordReader));
  }

  /**
   * Returns wrapped RecordReaderIterator from the iterator parameter
   *
   * @param baseIterator
   * @return
   */
  static RecordReaderIterator from(Iterator<RecordReader> baseIterator) {
    return new RecordReaderIterator() {
      @Override
      public void close() throws Exception {
        if (baseIterator instanceof AutoCloseable) {
          ((AutoCloseable) baseIterator).close();
        }
      }

      @Override
      public boolean hasNext() {
        return baseIterator.hasNext();
      }

      @Override
      public RecordReader next() {
        return baseIterator.next();
      }

      @Override
      public void addRuntimeFilter(RuntimeFilter runtimeFilter) {}

      @Override
      public List<RuntimeFilter> getRuntimeFilters() {
        return Collections.emptyList();
      }

      @Override
      public void produceFromBuffered(boolean toProduce) {}
    };
  }

  /**
   * Joins two RecordReaderIterators
   *
   * @param it1
   * @param it2
   * @return
   */
  static RecordReaderIterator join(RecordReaderIterator it1, RecordReaderIterator it2) {
    return new RecordReaderIterator() {

      @Override
      public boolean hasNext() {
        return it1.hasNext() || it2.hasNext();
      }

      @Override
      public RecordReader next() {
        return it1.hasNext() ? it1.next() : it2.next();
      }

      @Override
      public void remove() {
        if (it1.hasNext()) {
          it1.remove();
        } else {
          it2.remove();
        }
      }

      @Override
      public void forEachRemaining(Consumer<? super RecordReader> action) {
        it1.forEachRemaining(action);
        it2.forEachRemaining(action);
      }

      @Override
      public void close() throws Exception {
        AutoCloseables.close(it1, it2);
      }

      @Override
      public void addRuntimeFilter(RuntimeFilter runtimeFilter) {
        it1.addRuntimeFilter(runtimeFilter);
        it2.addRuntimeFilter(runtimeFilter);
      }

      @Override
      public List<RuntimeFilter> getRuntimeFilters() {
        return Stream.concat(it1.getRuntimeFilters().stream(), it2.getRuntimeFilters().stream())
            .collect(Collectors.toList());
      }

      @Override
      public void produceFromBuffered(boolean toProduce) {
        it1.produceFromBuffered(toProduce);
        it2.produceFromBuffered(toProduce);
      }
    };
  }

  RecordReaderIterator EMPTY_RECORD_ITERATOR = from(Collections.emptyIterator());
}
