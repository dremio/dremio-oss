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

import com.dremio.exec.store.RuntimeFilter;
import com.dremio.exec.store.dfs.SplitReaderCreator;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * SplitReaderCreatorIterator is an iterator that is supplied to the prefetching iterator and
 * creates the splitreadercreator
 */
public interface SplitReaderCreatorIterator extends Iterator<SplitReaderCreator>, AutoCloseable {
  /**
   * Add a runtime filter to the iterator
   *
   * @param runtimeFilter
   */
  default void addRuntimeFilter(RuntimeFilter runtimeFilter) {}

  /**
   * Return all the runtimefilters added to the iterator
   *
   * @return
   */
  default List<RuntimeFilter> getRuntimeFilters() {
    return Collections.emptyList();
  }

  /**
   * Mark the iterator to produce from any buffered splits
   *
   * @param toProduce
   */
  default void produceFromBufferedSplits(boolean toProduce) {}
}
