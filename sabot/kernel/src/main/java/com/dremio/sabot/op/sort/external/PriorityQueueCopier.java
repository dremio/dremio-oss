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
package com.dremio.sabot.op.sort.external;

import java.io.IOException;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.sort.external.DiskRunManager.DiskRunIterator;

public interface PriorityQueueCopier extends AutoCloseable, MovingCopier {
  static final long INITIAL_ALLOCATION = 10000000;
  static final long MAX_ALLOCATION = 20000000;

  final static TemplateClassDefinition<PriorityQueueCopier> TEMPLATE_DEFINITION = new TemplateClassDefinition<>(PriorityQueueCopier.class, PriorityQueueCopierTemplate.class);

  void setup(
      FunctionContext context,
      BufferAllocator allocator,
      DiskRunIterator[] iterators,
      VectorAccessible incoming,
      VectorContainer outgoing) throws SchemaChangeException, IOException ;

  int copy(int targetRecordCount);
}
