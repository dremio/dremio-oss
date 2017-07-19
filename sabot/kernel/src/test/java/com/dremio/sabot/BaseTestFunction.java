/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.sabot;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.ValueVector;

import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.fn.interpreter.InterpreterEvaluator;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.record.VectorContainer;
import com.dremio.sabot.Fixtures.Table;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.op.project.ProjectOperator;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.google.common.base.Preconditions;

public class BaseTestFunction extends BaseTestOperator {

  public void testFunctions(Object[][] tests){
    for(Object[] test : tests){
      testFunction((String) test[0], Arrays.copyOfRange(test, 1, test.length));
    }
  }


  /**
   * Evaluate the given expression using the provided inputs and confirm it results in the expected output. Runs both compiled and interpreted tests.
   * @param expr The expression to evaluate Note that the expression must refer to all the inputs as c0, c1, c2... cn at least once.
   * @param fields All of the input values (n-1), plus the output value (nth value).
   */
  public void testFunction(String stringExpression, Object... fieldsArr) {
    try{
      Preconditions.checkArgument(fieldsArr.length > 0, "Must provide an output for a function.");
      final List<Object> fields = Arrays.asList(fieldsArr);

      final LogicalExpression expr = toExpr(stringExpression);
      FieldExpressionCollector collector = new FieldExpressionCollector();
      final List<Object> inputs = fields.subList(0, fields.size() - 1);
      expr.accept(collector, null);
      Preconditions.checkArgument(collector.paths.size() == inputs.size(), "Expected number of fields referenced (%s) to equal number of inputs passed (%s).", collector.paths.size(), inputs.size());

      // check all the input paths.
      String[] names = new String[inputs.size()];

      for(int i = 0; i < inputs.size(); i++){
        String name = "c" + Integer.toString(i);
        Preconditions.checkArgument(collector.paths.contains(name), "Expected path (%s) not referenced.", name);
        names[i] = name;
      }

      final Table input = Fixtures.t(Fixtures.th(names), Fixtures.tr(inputs.toArray(new Object[inputs.size()])));
      final Table output = Fixtures.t(Fixtures.th("out"), Fixtures.tr(fieldsArr[fieldsArr.length - 1]));
      Project p = new Project(Arrays.asList(new NamedExpression(expr, new FieldReference("out"))), null);

      try {
        validateSingle(p, ProjectOperator.class, input.toGenerator(getTestAllocator()), output, DEFAULT_BATCH);
      } catch(AssertionError | Exception e) {
        throw new RuntimeException("Failure while testing function using code compilation.", e);
      }

      try{
         testInterp(p, expr, input, output);
      }catch(AssertionError | Exception e){
        throw new RuntimeException("Failure while testing function using code interpretation.", e);
      }

    }catch(AssertionError | Exception e){
      throw new RuntimeException(String.format("Failure while attempting to evaluate expr [%s] using inputs/outputs of %s.", stringExpression, Arrays.toString(fieldsArr)), e);
    }


    System.out.println("Passed: " + stringExpression);
  }

  private void testInterp(Project project, LogicalExpression expr, Table input, Table expected) throws Exception {
    final BufferAllocator childAllocator = getTestAllocator().newChildAllocator("interp", 0, Long.MAX_VALUE);
    try(OperatorContextImpl context = testContext.getNewOperatorContext(childAllocator, project, 1);
        Generator generator = input.toGenerator(context.getAllocator());
        VectorContainer output = new VectorContainer(context.getAllocator());
        ){

      LogicalExpression materializedExpr = ExpressionTreeMaterializer.materializeAndCheckErrors(expr, generator.getOutput().getSchema(), testContext.getFunctionLookupContext());

      ValueVector vector = output.addOrGet(materializedExpr.getCompleteType().toField("out"));
      vector.allocateNew();
      output.buildSchema();
      generator.next(1);
      InterpreterEvaluator.evaluate(generator.getOutput(), context.getFunctionContext(), vector, materializedExpr);
      output.setAllCount(1);
      try(RecordBatchData data = new RecordBatchData(output, context.getAllocator())){
        expected.checkValid(Collections.singletonList(data));
      }

    }

  }
//
//  private void testCompiledFunction(String stringExpression, Object... fields) {
//
//  }

  private class FieldExpressionCollector extends AbstractExprVisitor<Void, Void, RuntimeException> {

    private Set<String> paths = new HashSet<>();

    @Override
    public Void visitSchemaPath(SchemaPath path, Void value) throws RuntimeException {
      paths.add(path.getAsUnescapedPath());
      return null;
    }


    @Override
    public Void visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      for(LogicalExpression child : e) {
        child.accept(this,  null);
      }
      return null;
    }

  }


}
