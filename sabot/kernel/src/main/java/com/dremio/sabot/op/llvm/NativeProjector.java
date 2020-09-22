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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.arrow.gandiva.evaluator.Projector;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.gandiva.expression.ExpressionTree;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class NativeProjector implements AutoCloseable {
  private final VectorAccessible incoming;
  private List<ExpressionTree> columnExprList = new ArrayList<>();
  private Projector projector = null;
  private VectorSchemaRoot root;
  private final Schema schema;
  private final FunctionContext functionContext;
  private final Set<Field> referencedFields;
  private final boolean optimize;

  NativeProjector(VectorAccessible incoming, Schema schema, FunctionContext functionContext, boolean optimize) {
    this.incoming = incoming;
    this.schema = schema;
    this.functionContext = functionContext;
    // preserve order of insertion
    referencedFields = Sets.newLinkedHashSet();
    this.optimize = optimize;
  }

  public void add(LogicalExpression expr, FieldVector outputVector) {
    final ExpressionTree tree = GandivaExpressionBuilder.serializeExpr(incoming, expr,
      outputVector, referencedFields, functionContext);
    columnExprList.add(tree);
  }

  public void build() throws GandivaException {
    root = GandivaUtils.getSchemaRoot(incoming, referencedFields);
    projector = Projector.make(root.getSchema(), columnExprList, optimize);
  }

  public void execute(int recordCount, List<ValueVector> outVectors) throws Exception {
    root.setRowCount(recordCount);

    List<ArrowBuf> buffers = Lists.newArrayList();
    for (FieldVector v : root.getFieldVectors()) {
      buffers.addAll(v.getFieldBuffers());
    }

    projector.evaluate(recordCount, buffers, outVectors);
  }

  @Override
  public void close() throws Exception {
    projector.close();
  }
}
