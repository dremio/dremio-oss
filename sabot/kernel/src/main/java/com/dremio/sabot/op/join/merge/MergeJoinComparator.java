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

import org.apache.calcite.rel.core.JoinRelType;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.join.merge.MergeJoinOperator.InternalState;

/**
 * Interface for the merge join comparator
 *
 * State machine during inner join
 *
 * NEEDS_SETUP ---> OUT_OF_LOOPS (setup)
 *
 * OUT_OF_LOOPS --> OUT_OF_LOOPS (need data)
 * OUT_OF_LOOPS --> IN_INNER_LOOP (find match in continueJoin)
 *
 * IN_OUTER_LOOP --> IN_OUTER_LOOP (need data, left)
 * IN_OUTER_LOOP --> IN_INNER_LOOP (find match in continueJoin)
 *
 * IN_INNER_LOOP --> IN_OUTER_LOOP (does not match in continueJoin)
 * IN_INNER_LOOP --> IN_OUTER_LOOP (need data, right, reach end of right table)
 * IN_INNER_LOOP --> IN_OUTER_LOOP (need data, right, not the end)
 *
 */
public interface MergeJoinComparator {
  public final static TemplateClassDefinition<MergeJoinComparator> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(MergeJoinComparator.class, MergeJoinComparatorTemplate.class);

  /**
   * setup comparator by providing left and right iterators where batch logic is included
   * @param functionContext
   * @param joinType
   * @param leftIterator
   * @param rightIterator
   * @param outgoing
   * @param targetRecordsPerBatch
   */
  void setupMergeJoin(
      FunctionContext functionContext,
      JoinRelType joinType,
      MarkedAsyncIterator leftIterator,
      MarkedAsyncIterator rightIterator,
      VectorContainer outgoing,
      int targetRecordsPerBatch);

  /**
   * describe current location in the looping algorithm
   * @return internal state of the comparator
   */
  InternalState getInternalState();

  /**
   * @return output batch size
   */
  int getOutputSize();

  /**
   * continue join until having enough data or reach the end of either batch
   * it always start with the assumption that corresponding iterator's peek() will return the batch needed
   */
  void continueJoin();

  /**
   * used for non-inner joins
   * @return if we have finished processing all available non-matching records
   */
  boolean finishNonMatching();

  /**
   * reset output batch size counter
   */
  void resetOutputCounter();

}
