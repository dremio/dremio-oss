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
package com.dremio.sabot.op.join.merge;

import org.apache.commons.lang3.tuple.Pair;

import com.dremio.exec.record.VectorAccessible;
import com.google.common.collect.PeekingIterator;

/**
 * Describes marked iterator used in sort-merge join; it is compatible with the batch data
 * processing interface
 *
 * Because sort-merge join requires cartesian product of identical-key sets from
 * both left and right tables, we need to be able to backtrace records from previous batches
 *
 */
interface MarkedAsyncIterator extends PeekingIterator<Pair<VectorAccessible, Integer>>, AutoCloseable {

  /**
   * mark current point of iteration
   */
  void mark();

  /**
   * peek marked location
   * @return a pair of batch and index within the batch
   */
  Pair<VectorAccessible, Integer> peekMark();

  /**
   * restore to the state when mark() is called the last time
   */
  void resetToMark();

  /**
   * remove mark
   */
  void clearMark();

  /**
   * @return a dummy batch that follows the row type, etc
   */
  VectorAccessible getDummyBatch();

  /**
   * Add next batch to iterator, and return a callback to be called when
   * data loadable in this call will be released
   *
   * It is not allowed to call asyncPopulateBatch before executing Runnable
   * returned, i.e. one batch at a time
   * @param batch a batch that will not be released until callback is executed
   * @return callback before batch needs to be released
   */
  Runnable asyncAcceptBatch(VectorAccessible batch);

}
