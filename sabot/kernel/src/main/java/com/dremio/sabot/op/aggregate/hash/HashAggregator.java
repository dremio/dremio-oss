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
package com.dremio.sabot.op.aggregate.hash;

import java.io.IOException;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.expr.ClassProducer;
import com.dremio.exec.physical.config.HashAggregate;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.OperatorStats;
import com.dremio.sabot.op.common.hashtable.HashTableConfig;

public interface HashAggregator extends AutoCloseable {

  public static TemplateClassDefinition<HashAggregator> TEMPLATE_DEFINITION =
      new TemplateClassDefinition<HashAggregator>(HashAggregator.class, HashAggTemplate.class);

  void setup(
      HashAggregate hashAggrConfig,
      HashTableConfig htConfig,
      ClassProducer producer,
      OperatorStats stats,
      BufferAllocator allocator,
      VectorAccessible incoming,
      LogicalExpression[] valueExprs,
      List<TypedFieldId> valueFieldIds,
      TypedFieldId[] groupByOutFieldIds,
      VectorContainer outContainer) throws SchemaChangeException, ClassTransformationException, IOException;

  void addBatch(int records);

  int batchCount();

  int outputBatch(int batchIndex);

  /**
   * @return The number of entries in the hashtable
   */
  long numHashTableEntries();
}
