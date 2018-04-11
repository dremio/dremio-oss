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
package com.dremio.sabot.op.aggregate.streaming;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.op.aggregate.streaming.StreamingAggOperator.TransferOnDeckAtBat;

public interface StreamingAggregator {

  public static TemplateClassDefinition<StreamingAggregator> TEMPLATE_DEFINITION = new TemplateClassDefinition<StreamingAggregator>(StreamingAggregator.class, StreamingAggTemplate.class);

  void setup(
      FunctionContext context,
      VectorAccessible onDeck,
      VectorAccessible atBat,
      VectorAccessible output,
      int targetBatchSize,
      TransferOnDeckAtBat transfer);

  /**
   * Consume the first record at bat. Should only be called for the very first record received.
   */
  void consumeFirstRecord();

  /**
   * Consume the first record of onDeck records. Should only be called in case that we've already consumed at least one record previously.
   *
   * Compares the last record in atBat to the first record of onDeck.
   *
   * Expects that the new data is in onDeck and the previous data is atBat. Note that this operation also promotes the movement of data from onDeck to atBat.
   */
  int consumeBoundaryRecordAndSwap();

  /**
   * Consume all records in atBat until the outgoing batch is full excluding the very first record. We skip the first record since it will be consumed throguh either consumeFirstRecord or
   * @return The number of records in the outgoing batch or zero if an outgoing batch is not yet ready.
   */
  int consumeRecords();

  /**
   * There are no more records. Close the last aggregation and produce a batch of records.
   * @return
   */
  int closeLastAggregation();

}
