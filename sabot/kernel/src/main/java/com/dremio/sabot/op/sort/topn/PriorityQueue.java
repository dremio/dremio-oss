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
package com.dremio.sabot.op.sort.topn;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.sort.external.Sv4HyperContainer;
import org.apache.arrow.memory.BufferAllocator;

public interface PriorityQueue extends AutoCloseable {
  void add(RecordBatchData batch);

  void init(
      Sv4HyperContainer hyperBatch,
      int limit,
      FunctionContext context,
      BufferAllocator allocator,
      boolean hasSv2,
      int maxSize);

  void generate();

  Sv4HyperContainer getHyperBatch();

  SelectionVector4 getHeapSv4();

  SelectionVector4 getFinalSv4();

  void resetQueue(final VectorContainer newQueue, final SelectionVector4 oldHeap);

  static TemplateClassDefinition<PriorityQueue> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<PriorityQueue>(PriorityQueue.class, PriorityQueueTemplate.class);
}
