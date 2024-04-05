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
package com.dremio.sabot.op.windowframe;

import com.dremio.exec.compile.TemplateClassDefinition;
import com.dremio.exec.exception.SchemaChangeException;
import com.dremio.exec.physical.config.WindowPOP;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.exec.context.FunctionContext;
import com.dremio.sabot.exec.context.OperatorContext;
import java.util.List;
import javax.inject.Named;

public interface WindowFramer extends AutoCloseable {
  TemplateClassDefinition<WindowFramer> NOFRAME_TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(WindowFramer.class, NoFrameSupportTemplate.class);
  TemplateClassDefinition<WindowFramer> FRAME_TEMPLATE_DEFINITION =
      new TemplateClassDefinition<>(WindowFramer.class, FrameSupportTemplate.class);

  void setup(
      final List<VectorContainer> batches,
      final VectorAccessible container,
      final OperatorContext operatorContext,
      final boolean requireFullPartition,
      final WindowPOP popConfig,
      FunctionContext context)
      throws SchemaChangeException;

  /**
   * process the inner batch and write the aggregated values in the container
   *
   * @throws Exception
   */
  void doWork(int batchIndex) throws Exception;

  /**
   * @return number rows processed in last batch
   */
  int getOutputCount();

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the
   * partition by expression
   *
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  boolean isSamePartition(
      @Named("b1Index") int b1Index,
      @Named("b1") VectorAccessible b1,
      @Named("b2Index") int b2Index,
      @Named("b2") VectorAccessible b2);

  /**
   * compares two rows from different batches (can be the same), if they have the same value for the
   * order by expression
   *
   * @param b1Index index of first row
   * @param b1 batch for first row
   * @param b2Index index of second row
   * @param b2 batch for second row
   * @return true if the rows are in the same partition
   */
  boolean isPeer(
      @Named("b1Index") int b1Index,
      @Named("b1") VectorAccessible b1,
      @Named("b2Index") int b2Index,
      @Named("b2") VectorAccessible b2);
}
