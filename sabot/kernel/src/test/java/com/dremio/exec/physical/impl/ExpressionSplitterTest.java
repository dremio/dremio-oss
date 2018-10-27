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
package com.dremio.exec.physical.impl;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.EvaluationType;
import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.expr.ExpressionSplitHelper;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;
import com.google.common.collect.Lists;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Unit test cases for the expression splitter
 */
public class ExpressionSplitterTest extends BaseTestFunction {
  // Private class to verify the output of the split
  private class Split {
    boolean useGandiva;
    String name;
    String expr;

    Split(boolean useGandiva, String name, String expr) {
      this.useGandiva = useGandiva;
      this.name = name;
      this.expr = expr;
    }
  }

  // Helper function to generate the schema from a array of fields
  private VectorContainer getVectorContainer(Object... fieldsArr) {
    final List<Object> inputs = Arrays.asList(fieldsArr);

    Fixtures.DataBatch b = new Fixtures.DataBatch(Fixtures.tr(inputs.toArray(new Object[inputs.size()])));
    String[] names = new String[inputs.size()];
    for(int i = 0; i < inputs.size(); i++) {
      String name = "c" + i;
      names[i] = name;
    }
    Field[] fields = Fixtures.getFields(Fixtures.th(names), b);
    VectorContainer vectorContainer = new VectorContainer(getTestAllocator());
    for(int i = 0; i < fields.length; i++){
      vectorContainer.addOrGet(fields[i]);
    }
    vectorContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);

    return vectorContainer;
  }

  // Splits the expression
  // First, the expression is materialized (using Java only)
  // Second, the expression is annotated using a custom annotator for Gandiva support
  // Lastly, the expression is split
  private List<NamedExpression> splitExpression(LogicalExpression expr, VectorContainer vectorContainer, GandivaAnnotator annotator)
    throws Exception
  {
    // materialize expression - not annotated with Gandiva
    try(ErrorCollector errorCollector = new ErrorCollectorImpl()){
      expr = ExpressionTreeMaterializer.materialize(expr, vectorContainer.getSchema(), errorCollector, testContext.getFunctionLookupContext(), false);
    }

    // annotate with Gandiva
    expr.accept(annotator, null);

    // split the annotated expression
    ExpressionSplitter splitter = new ExpressionSplitter((VectorContainer)vectorContainer, (ExpressionSplitHelper)annotator, "_xxx");
    List<NamedExpression> splits = splitter.splitExpression(expr);
    return splits;
  }

  // Split the query and match the splits against expected splits
  private void splitAndVerify(Object[] query, Split[] expSplits, GandivaAnnotator annotator) throws Exception {
    // find the schema for the expression
    VectorContainer vectorContainer = getVectorContainer(Arrays.copyOfRange(query, 1, query.length));
    // parse string to expression
    LogicalExpression expr = toExpr((String)query[0]);

    List<NamedExpression> splits = splitExpression(expr, vectorContainer, annotator);

    // ExprToString is used to convert an expression to a string for comparison
    ExprToString stringBuilder = new ExprToString(vectorContainer);

    // verify number of splits
    assertEquals(expSplits.length, splits.size());

    // verify each split with the expected value
    for(int i = 0; i < splits.size(); i++) {
      NamedExpression split = (NamedExpression)splits.get(i);

      String expExpr = expSplits[i].expr.replaceAll("\\s+", "").toLowerCase();
      String actualExpr = stringBuilder.expr2String(split.getExpr()).replaceAll("\\s+", "").toLowerCase();

      assertEquals(expSplits[i].useGandiva, split.getExpr().isEvaluationTypeSupported(EvaluationType.ExecutionType.GANDIVA));
      assertEquals(expSplits[i].name, split.getRef().getAsUnescapedPath());
      assertEquals(expExpr, actualExpr);
    }
  }

  // test the same if-condition in 2 cases: Gandiva supports the if statement and Gandiva does not support the if statement
  private void testIfCondition(Object[] query, Split[] noIfSupport, Split[] withIfSupport, GandivaAnnotator annotator) throws
    Exception
  {
    splitAndVerify(query, noIfSupport, annotator);
    annotator.ifExprSupported = true;
    splitAndVerify(query, withIfSupport, annotator);
  }

  private void preserveShortCircuitsAndValidate(Object[] query, Split[] expSplits, GandivaAnnotator annotator) throws Exception
  {
    ExpressionSplitter.preserveShortCircuits = true;
    splitAndVerify(query, expSplits, annotator);
    ExpressionSplitter.preserveShortCircuits = false;
  }

  @Test
  public void splitIfCondition() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");
    Object[] query = {"case when c0 > c1 then c0 - c1 else c1 + c0 end", 10, 11};
    Split[] noIfSupport = {
      new Split(true, "_xxx0", "greater_than(c0, c1)"),
      new Split(false, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };

    Split[] withIfSupport = new Split[]{
      new Split(false, "_xxx0", "subtract(c0, c1)"),
      new Split(false, "_xxx1", "add(c1, c0)"),
      new Split(true, "_xxx2", "(if (greater_than(c0, c1)) then (_xxx0) else (_xxx1) end)")
    };

    testIfCondition(query, noIfSupport, withIfSupport, annotator);

    Split[] preserveShortCircuits = new Split[] {
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    preserveShortCircuitsAndValidate(query, preserveShortCircuits, annotator);
  }

  @Test
  public void splitThenExpression() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("subtract");
    Object[] query = {"case when c0 > c1 then c0 - c1 else c1 + c0 end", 10, 11};
    Split[] noIfSupport = {
      new Split(true, "_xxx0", "subtract(c0, c1)"),
      new Split(false, "_xxx1", "(if (greater_than(c0, c1)) then (_xxx0) else (add(c1, c0)) end)")
    };

    Split[] withIfSupport = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)"),
      new Split(false, "_xxx1", "add(c1, c0)"),
      new Split(true, "_xxx2", "(if (_xxx0) then (subtract(c0, c1)) else (_xxx1) end)")
    };

    testIfCondition(query, noIfSupport, withIfSupport, annotator);

    Split[] preserveShortCircuits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    preserveShortCircuitsAndValidate(query, preserveShortCircuits, annotator);
  }

  @Test
  public void splitElseExpression() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("add");
    Object[] query = {"case when c0 > c1 then c0 - c1 else c1 + c0 end", 10, 11};
    Split[] noIfSupport = {
      new Split(true, "_xxx0", "add(c1, c0)"),
      new Split(false, "_xxx1", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (_xxx0) end)")
    };

    Split[] withIfSupport = {
      new Split(false, "_xxx0", "greater_than(c0, c1)"),
      new Split(false, "_xxx1", "subtract(c0, c1)"),
      new Split(true, "_xxx2", "(if (_xxx0) then (_xxx1) else (add(c1, c0)) end)")
    };

    testIfCondition(query, noIfSupport, withIfSupport, annotator);

    Split[] preserveShortCircuits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    preserveShortCircuitsAndValidate(query, preserveShortCircuits, annotator);
  }

  @Test
  public void splitThenAndElse() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("add", "subtract");
    Object[] query = {"case when c0 > c1 then c0 - c1 else c1 + c0 end", 10, 11};
    Split[] noIfSupport = {
      new Split(true, "_xxx0", "subtract(c0, c1)"),
      new Split(true, "_xxx1", "add(c1, c0)"),
      new Split(false, "_xxx2", "(if (greater_than(c0, c1)) then (_xxx0) else (_xxx1) end)")
    };

    Split[] withIfSupport = {
      new Split(false, "_xxx0", "greater_than(c0, c1)"),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };

    testIfCondition(query, noIfSupport, withIfSupport, annotator);

    Split[] preserveShortCircuits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    preserveShortCircuitsAndValidate(query, preserveShortCircuits, annotator);
  }

  @Test
  public void noSplitIf() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("add", "subtract", "greater_than");
    Object[] query = {"case when c0 > c1 then c0 - c1 else c1 + c0 end", 10, 11};

    annotator.ifExprSupported = true;
    Split[] splits = {
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    splitAndVerify(query, splits, annotator);

    Split[] preserveShortCircuits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)")
    };
    preserveShortCircuitsAndValidate(query, preserveShortCircuits, annotator);
  }

  @Test
  public void testFunctionArgs() throws Exception {
    Object[] query = {"((c0 + c1) * c2) + (c0 - c1)", 10, 11, 12};
    GandivaAnnotator annotator = new GandivaAnnotator("add", "subtract");
    Split[] splits = {
      new Split(true, "_xxx0", "add(c0, c1)"),
      new Split(false, "_xxx1", "multiply(_xxx0, c2)"),
      new Split(true, "_xxx2", "add(_xxx1, subtract(c0, c1))")
    };

    splitAndVerify(query, splits, annotator);

    // support all functions and test
    annotator = new GandivaAnnotator("add", "subtract", "multiply");
    splits = new Split[]{
      new Split(true, "_xxx0", "add(multiply(add(c0, c1), c2), subtract(c0, c1))")
    };

    splitAndVerify(query, splits, annotator);
  }

  @Test
  public void testBooleanOperator() throws Exception {
    Object[] query = {"(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)", 18L};
    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    Split[] splits = {
      new Split(true, "_xxx0", "booleanAnd(greater_than(c0, castBIGINT(10i)), less_than(c0, castBIGINT(20i)))"),
      new Split(true, "_xxx1", "equal(mod(c0, 2i), 0i)"),
      new Split(false, "_xxx2", "booleanOr(_xxx0, _xxx1)")
    };

    splitAndVerify(query, splits, annotator);

    splits = new Split[]{
      new Split(false, "_xxx0", "booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i))), equal(mod(c0, 2i),0i))")
    };
    preserveShortCircuitsAndValidate(query, splits, annotator);
  }

  @Test
  public void testTree() throws Exception {
    Object[] query = {"(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)", 18L};
    // the query translates to the following: split it at different places and verify
    // booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i)), equal(mod(c0, 2i), 0i))

    // support all except castBIGINT
    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "booleanOr", "greater_than", "less_than", "mod", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "castBIGINT(10i)"),
      new Split(false, "_xxx1", "castBIGINT(20i)"),
      new Split(true, "_xxx2", "booleanOr(booleanAnd(greater_than(c0, _xxx0),less_than(c0, _xxx1)), equal(mod(c0, 2i),0i))")
    };

    splitAndVerify(query, splits, annotator);

    // support all functions
    annotator = new GandivaAnnotator("booleanAnd", "booleanOr", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i))), equal(mod(c0, 2i),0i))")
    };
    splitAndVerify(query, splits, annotator);

    // support all except booleanAnd and mod
    annotator = new GandivaAnnotator("booleanOr", "greater_than", "less_than", "equal", "castBIGINT");
    splits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, castBIGINT(10i))"),
      new Split(true, "_xxx1", "less_than(c0, castBIGINT(20i))"),
      new Split(false, "_xxx2", "booleanAnd(_xxx0, _xxx1)"),
      new Split(false, "_xxx3", "mod(c0, 2i)"),
      new Split(true, "_xxx4", "booleanOr(_xxx2, equal(_xxx3,0i))")
    };
    splitAndVerify(query, splits, annotator);
  }

  @Test
  public void testCannotSplit() throws Exception {
    Object[] query = {"(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)", 18L};
    // the query translates to the following: split it at different places and verify
    // booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i)), equal(mod(c0, 2i), 0i))

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "less_than");
    Split[] splits = new Split[]{
      new Split(false, "_xxx0", "castBIGINT(10i)"),
      new Split(true, "_xxx1", "greater_than(c0, _xxx0)"),
      new Split(false, "_xxx2", "castBIGINT(20i)"),
      new Split(true, "_xxx3", "less_than(c0, _xxx2)"),
      new Split(false, "_xxx4", "booleanOr(booleanAnd(_xxx1, _xxx3),equal(mod(c0, 2i), 0i))")
    };
    splitAndVerify(query, splits, annotator);

    // less than cannot be the root of a split
    // the less_than cannot be split and run in Gandiva. Hence, the split will happen at the booleanAnd
    annotator.setRootFunctions("less_than");
    splits = new Split[]{
      new Split(false, "_xxx0", "castBIGINT(10i)"),
      new Split(true, "_xxx1", "greater_than(c0, _xxx0)"),
      new Split(false, "_xxx2", "booleanOr(booleanAnd(_xxx1, less_than(c0, castBIGINT(20i))),equal(mod(c0, 2i), 0i))")
    };
    splitAndVerify(query, splits, annotator);
  }

  // Converts an expression to a tree
  // Uses the schema to convert reads to column names
  private class ExprToString extends ExpressionStringBuilder {
    VectorContainer vectorContainer;

    ExprToString(VectorContainer vectorContainer) {
      super();
      this.vectorContainer = vectorContainer;
    }

    public String expr2String(LogicalExpression expr) {
      StringBuilder sb = new StringBuilder();
      expr.accept(this, sb);
      return sb.toString();
    }

    @Override
    public Void visitUnknown(LogicalExpression e, StringBuilder sb) {
      if (e instanceof ValueVectorReadExpression) {
        ValueVectorReadExpression readExpression = (ValueVectorReadExpression)e;

        VectorWrapper wrapper = this.vectorContainer.getValueAccessorById(FieldVector.class, readExpression.getFieldId().getFieldIds());
        sb.append(wrapper.getField().getName());
      }

      return null;
    }
  }

  // Custom visitor to annotate an expression with Gandiva support
  // Takes a list of supported functions
  private class GandivaAnnotator extends AbstractExprVisitor<Void,Void,Exception> implements ExpressionSplitHelper {
    // List of functions supported by Gandiva
    private final List<String> supportedFunctions;
    // List of functions that can be tree-roots
    private final List<String> rootFunctions;
    // Is the if-statement supported by Gandiva
    private boolean ifExprSupported = false;

    GandivaAnnotator(String... supportedFunctions) {
      this.supportedFunctions = Lists.newArrayList();
      this.rootFunctions = Lists.newArrayList();
      List<Object> functions = Arrays.asList(supportedFunctions);
      copyStringsTo(this.supportedFunctions, functions);
    }

    void copyStringsTo(List<String> stringList, List<Object> inList) {
      for(int i = 0; i < inList.size(); i++) {
        String fnName = (String)inList.get(i);
        stringList.add(new String(fnName.toLowerCase()));
      }
    }

    void setRootFunctions(String... rootFunctions) {
      List<Object> functions = Arrays.asList(rootFunctions);
      copyStringsTo(this.rootFunctions, functions);
    }

    @Override
    public boolean canSplitAt(LogicalExpression e) {
      String fnName = null;
      if (e instanceof FunctionHolderExpression) {
        fnName = ((FunctionHolderExpression)e).getName();
      }

      if (e instanceof BooleanOperator) {
        fnName = ((BooleanOperator)e).getName();
      }

      if (fnName == null) {
        return true;
      }

      // Dont allow split if the tree root is one of the functions in rootFunctions
      return (!rootFunctions.contains(fnName.toLowerCase()));
    }

    @Override
    public Void visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws Exception {
      for(LogicalExpression e : holder.args) {
        e.accept(this, null);
      }

      if (this.supportedFunctions.contains(holder.getName().toLowerCase())) {
        holder.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      return null;
    }

    @Override
    public Void visitIfExpression(IfExpression ifExpr, Void value) throws Exception {
      ifExpr.ifCondition.condition.accept(this, null);
      ifExpr.ifCondition.expression.accept(this, null);
      ifExpr.elseExpression.accept(this, null);

      if (ifExprSupported) {
        ifExpr.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }
      return null;
    }

    @Override
    public Void visitBooleanOperator(BooleanOperator op, Void value) throws Exception {
      for(LogicalExpression e : op.args) {
        e.accept(this, null);
      }

      if (this.supportedFunctions.contains(op.getName().toLowerCase())) {
        op.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }

      return null;
    }

    @Override
    public Void visitUnknown(LogicalExpression e, Void value) throws Exception {
      if (e instanceof ValueVectorReadExpression) {
        // let Gandiva support all reads
        e.addEvaluationType(EvaluationType.ExecutionType.GANDIVA);
      }
      return null;
    }
  }
}
