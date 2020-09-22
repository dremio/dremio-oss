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
package com.dremio.sabot.op.sort.external;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.SimpleIntVector;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.ExpandableHyperContainer;
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.selection.SelectionVector4;
import com.dremio.sabot.exec.context.FunctionContext;

public interface QuickSorterInterface extends AutoCloseable {
  static TemplateClassDefinition<QuickSorterInterface> TEMPLATE_DEFINITION =
    new TemplateClassDefinition<QuickSorterInterface>(QuickSorterInterface.class, QuickSorterTemplate.class);

  void init(FunctionContext context, ExpandableHyperContainer hyperContainer) throws SchemaChangeException;
  void add(final RecordBatchData batch) throws SchemaChangeException;
  SelectionVector4 getFinalSort(BufferAllocator allocator, int targetBatchSize);
  ExpandableHyperContainer getHyperBatch();
  void setDataBuffer(SimpleIntVector intVectorBuffer);
  void close() throws Exception;
}
