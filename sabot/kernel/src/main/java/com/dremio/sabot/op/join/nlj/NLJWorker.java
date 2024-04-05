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
package com.dremio.sabot.op.join.nlj;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import java.util.LinkedList;

/** Interface for the nested loop join operator. */
public interface NLJWorker {
  public static TemplateClassDefinition<NLJWorker> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(NLJWorker.class, NLJWorkerTemplate.class);

  void setupNestedLoopJoin(
      FunctionContext context,
      VectorAccessible left,
      ExpandableHyperContainer rightContainer,
      LinkedList<Integer> rightCounts,
      VectorAccessible outgoing);

  /**
   * Starting at the provided index, attempt to output records using all right batches and the
   * current left batch up to the target total output.
   *
   * @param outputIndex The outgoing batch index to start with.
   * @param targetTotalOutput The target amount of records to output.
   * @return The update output index. A positive value if all remaining matches from the current
   *     left batch were matched. A negative version if we ran out of space while outputting. Zero
   *     if we started with a zero output index and output no records.
   */
  int emitRecords(int outputIndex, int targetTotalOutput);
}
