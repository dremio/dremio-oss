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
package com.dremio.exec.planner;

import com.dremio.exec.physical.base.AbstractPhysicalVisitor;
import com.dremio.exec.physical.base.AbstractWriter;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.Root;
import com.dremio.exec.physical.base.Writer;
import com.dremio.exec.physical.config.WriterCommitterPOP;
import com.dremio.exec.record.BatchSchema;

/** Find correct schema for root operator (below writer if a writer exists). */
public class RootSchemaFinder extends AbstractPhysicalVisitor<Void, Void, Exception> {

  private BatchSchema batchSchema;

  public static BatchSchema getSchema(Root rootOperator) throws Exception {
    // Start with schema of root operator. Replace it with schema of writer operator if any writer
    // exists
    final BatchSchema rootOperatorSchema = rootOperator.getProps().getSchema();
    final RootSchemaFinder rootSchemaFinder = new RootSchemaFinder();
    rootOperator.accept(rootSchemaFinder, null);
    if (rootSchemaFinder.batchSchema != null) {
      return rootSchemaFinder.batchSchema;
    }

    return rootOperatorSchema;
  }

  private RootSchemaFinder() {}

  @Override
  public Void visitWriterCommiter(WriterCommitterPOP commiter, Void value) throws Exception {
    return super.visitWriterCommiter(commiter, value);
  }

  @Override
  public Void visitWriter(Writer writer, Void value) throws Exception {
    this.batchSchema = ((AbstractWriter) writer).getChild().getProps().getSchema();
    return visitOp(writer, value);
  }

  @Override
  public Void visitOp(PhysicalOperator op, Void value) throws Exception {
    return super.visitChildren(op, value);
  }
}
