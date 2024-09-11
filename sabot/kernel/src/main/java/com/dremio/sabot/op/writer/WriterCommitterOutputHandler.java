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
package com.dremio.sabot.op.writer;

import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.spi.SingleInputOperator;

/**
 * Abstraction for {@link WriterCommitterOperator} to have different output writing strategies.
 * Irrespective of the strategy, the output schema is always fixed to {@link
 * com.dremio.exec.store.RecordWriter#SCHEMA}
 */
public interface WriterCommitterOutputHandler extends AutoCloseable {

  VectorAccessible setup(VectorAccessible accessible) throws Exception;

  SingleInputOperator.State getState();

  int outputData() throws Exception;

  void noMoreToConsume() throws Exception;

  void consumeData(int records) throws Exception;

  /**
   * Writes the values in the outgoing vector. Applicable only in the implementations that support
   * custom output.
   */
  void write(WriterCommitterRecord rec);

  /** Factory method for instantiating an implementation */
  static WriterCommitterOutputHandler getInstance(
      OperatorContext context, WriterCommitterPOP config, boolean hasCustomOutput) {
    return hasCustomOutput
        ? new CustomOutputHandler(context, config)
        : new ProjectOutputHandler(context, config);
  }
}
