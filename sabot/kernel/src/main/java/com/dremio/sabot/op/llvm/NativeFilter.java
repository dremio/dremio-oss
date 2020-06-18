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
package com.dremio.sabot.op.llvm;

import java.util.List;
import java.util.Set;

import org.apache.arrow.gandiva.evaluator.Filter;
import org.apache.arrow.gandiva.evaluator.SelectionVector;
import org.apache.arrow.gandiva.evaluator.SelectionVectorInt16;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.Condition;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.selection.SelectionVector2;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Adapter to gandiva filter.
 */
public class NativeFilter implements AutoCloseable {

  private final Filter filter;
  private final VectorSchemaRoot root;
  private final SelectionVector2 selectionVector;

  private NativeFilter(Filter filter, VectorSchemaRoot root, SelectionVector2 selectionVector) {
    this.filter = filter;
    this.root = root;
    this.selectionVector = selectionVector;
  }

  /**
   * Builds a gandiva filter for a given condition.
   * @param expr the filter expression
   * @param input the input container.
   * @param selectionVector - the output selection vector
   * @param stats
   * @return instance of Native Filter.
   * @throws GandivaException when we fail to make the gandiva filter
   */
  static public NativeFilter build(LogicalExpression expr, VectorAccessible input,
                                   SelectionVector2 selectionVector, FunctionContext functionContext) throws GandivaException {
    Set referencedFields = Sets.newHashSet();
    Condition condition = GandivaExpressionBuilder.serializeExprToCondition(input, expr, referencedFields, functionContext);
    VectorSchemaRoot root = GandivaUtils.getSchemaRoot(input, referencedFields);
    Filter filter = Filter.make(root.getSchema(), condition);
    return new NativeFilter(filter, root, selectionVector);
  }

  /**
   * Filter a batch of records against the expression.
   * @param recordCount - number of records to consume
   * @return the number of records that passed the filter
   * @throws GandivaException on evaluation exception.
   */
  public int filterBatch(int recordCount) throws GandivaException {
    if (recordCount == 0) {
      return 0;
    }

    root.setRowCount(recordCount);
    List<ArrowBuf> buffers = Lists.newArrayList();
    for (FieldVector v : root.getFieldVectors()) {
      buffers.addAll(v.getFieldBuffers());
    }

    selectionVector.allocateNew(recordCount);

    // do not take ownership of the buffer.
    ArrowBuf svBuffer = selectionVector.getBuffer(false);
    SelectionVector selectionVectorGandiva = new SelectionVectorInt16(svBuffer);

    filter.evaluate(recordCount, buffers, selectionVectorGandiva);
    selectionVector.setRecordCount(selectionVectorGandiva.getRecordCount());
    return selectionVector.getCount();
  }

  /**
   * Close the underlying gandiva filter.
   * @throws GandivaException
   */
  @Override
  public void close() throws GandivaException {
    filter.close();
  }

}

