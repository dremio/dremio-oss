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
package com.dremio.exec.planner;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.BatchSchema;

/**
 * Find correct schema for root operator (below writer if a writer exists).
 */
public class RootSchemaFinder extends AbstractPhysicalVisitor<Void, Void, Exception> {

  private final FunctionLookupContext functionLookupContext;
  private BatchSchema batchSchema;

  public static BatchSchema getSchema(Root rootOperator, FunctionLookupContext functionLookupContext) throws Exception {
    // Start with schema of root operator. Replace it with schema of writer operator if any writer exists
    final BatchSchema rootOperatorSchema = rootOperator.getSchema(functionLookupContext);
    final RootSchemaFinder rootSchemaFinder = new RootSchemaFinder(functionLookupContext);
    rootOperator.accept(rootSchemaFinder, null);
    if (rootSchemaFinder.batchSchema != null) {
      return rootSchemaFinder.batchSchema;
    }

    return rootOperatorSchema;
  }

  private RootSchemaFinder(FunctionLookupContext functionLookupContext) {
    this.functionLookupContext = functionLookupContext;
  }

  @Override
  public Void visitWriterCommiter(WriterCommitterPOP commiter, Void value) throws Exception {
    return super.visitWriterCommiter(commiter, value);
  }

  @Override
  public Void visitWriter(Writer writer, Void value) throws Exception {
    this.batchSchema = ((AbstractWriter) writer).getChild().getSchema(functionLookupContext);
    return visitOp(writer, value);
  }

  @Override
  public Void visitOp(PhysicalOperator op, Void value) throws Exception {
    return super.visitChildren(op, value);
  }
}
