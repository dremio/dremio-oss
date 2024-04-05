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

package com.dremio.exec.expr.fn;

import com.dremio.common.expression.CompleteType;
import com.dremio.exec.expr.ClassGenerator;
import com.dremio.exec.expr.ClassGenerator.HoldingContainer;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JVar;
import org.apache.arrow.vector.complex.writer.BaseWriter.ComplexWriter;

public class ComplexWriterFunctionHolder extends SimpleFunctionHolder {

  public ComplexWriterFunctionHolder(
      FunctionAttributes functionAttributes, FunctionInitializer initializer) {
    super(functionAttributes, initializer);
  }

  @Override
  protected HoldingContainer generateEvalBody(
      ClassGenerator<?> g,
      CompleteType resolvedOutput,
      HoldingContainer[] inputVariables,
      String body,
      JVar[] workspaceJVars) {

    g.getEvalBlock()
        .directStatement(
            String.format(
                "//---- start of eval portion of %s function. ----//", registeredNames[0]));

    JBlock sub = new JBlock(true, true);

    JVar complexWriter =
        g.declareClassField("complexWriter", g.getModel()._ref(ComplexWriter.class));

    final JExpression writerCreator = JExpr.direct("writerCreator");
    g.getSetupBlock()
        .assign(
            complexWriter,
            writerCreator.invoke("addComplexWriter").arg(g.getOutputReferenceName()));

    g.getEvalBlock()
        .add(complexWriter.invoke("setPosition").arg(g.getMappingSet().getValueWriteIndex()));

    sub.decl(g.getModel()._ref(ComplexWriter.class), getReturnName(), complexWriter);

    // add the subblock after the out declaration.
    g.getEvalBlock().add(sub);

    addProtectedBlock(g, sub, body, inputVariables, workspaceJVars, false);

    g.getEvalBlock()
        .directStatement(
            String.format("//---- end of eval portion of %s function. ----//", registeredNames[0]));

    return null;
  }
}
