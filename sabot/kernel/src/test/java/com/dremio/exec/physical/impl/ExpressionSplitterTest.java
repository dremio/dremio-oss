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

import static com.dremio.sabot.Fixtures.date;
import static com.dremio.sabot.Fixtures.t;
import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static com.dremio.sabot.Fixtures.ts;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.ValueExpressions;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.expr.CodeGenContext;
import com.dremio.exec.expr.ExpressionEvaluationOptions;
import com.dremio.exec.expr.ExpressionSplit;
import com.dremio.exec.expr.ExpressionSplitHelper;
import com.dremio.exec.expr.ExpressionSplitter;
import com.dremio.exec.expr.ExpressionTreeMaterializer;
import com.dremio.exec.expr.FunctionHolderExpr;
import com.dremio.exec.expr.ValueVectorReadExpression;
import com.dremio.exec.expr.fn.BaseFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionHolder;
import com.dremio.exec.expr.fn.GandivaFunctionHolderExpression;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.config.Project;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.dremio.sabot.op.sort.external.RecordBatchData;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Unit test cases for the expression splitter
 */
public class ExpressionSplitterTest extends BaseTestFunction {
  String ifQuery = "case when c0 > c1 then c0 - c1 else c1 + c0 end";

  // Evaluates the expression with a batch size of 2 for the inputs below
  // This evaluates the expression twice
  Fixtures.Table ifInput = Fixtures.split(
    th("c0", "c1"),
    2,
    tr(10, 11),
    tr(4, 3),
    tr(5, 5)
  );

  Fixtures.Table ifOutput = t(
    th("out"),
    tr(21),
    tr(1),
    tr(10)
  );

  // Private class to verify the output of the split
  private class Split {
    boolean useGandiva;
    String name;
    String expr;
    int execIteration;
    int numReaders;
    List<String> dependencies;

    Split(boolean useGandiva, String name, String expr, int iteration, int numReaders, String... dependencies) {
      this.useGandiva = useGandiva;
      this.name = name;
      this.expr = expr;
      this.execIteration = iteration;
      this.numReaders = numReaders;
      this.dependencies = Arrays.asList(dependencies);
    }
  }

  // Split the query and match the splits against expected splits
  //
  // Splits the expression
  // First, the expression is materialized (using Java only)
  // Second, the expression is annotated using a custom annotator for Gandiva support
  // Third, the expression is split and the splits verified
  // Fourth, the split expression evaluated and the output value verified
  private void splitAndVerify(LogicalExpression expr, Fixtures.Table input, Fixtures.Table output, Split[] expSplits, GandivaAnnotator annotator)
    throws Exception
  {
    // Get the allocator for allocating vectors
    BufferAllocator testAllocator = getTestAllocator();
    Generator generator = input.toGenerator(testAllocator);
    // find the schema for the expression
    VectorAccessible vectorContainer = generator.getOutput();

    Project pop = new Project(OpProps.prototype(), null, Arrays.asList(new NamedExpression(expr, new FieldReference("out"))));
    final BufferAllocator childAllocator = testAllocator.newChildAllocator(
      pop.getClass().getSimpleName(),
      pop.getProps().getMemReserve(),
      pop.getProps().getMemLimit() == 0 ? Long.MAX_VALUE : pop.getProps().getMemLimit());

    int batchSize = 1;
    final OperatorContextImpl context = testContext.getNewOperatorContext(childAllocator, pop, batchSize);
    testCloseables.add(context);

    // materialize expression
    try(ErrorCollector errorCollector = new ErrorCollectorImpl()){
      expr = ExpressionTreeMaterializer.materialize(expr, vectorContainer.getSchema(), errorCollector, testContext.getFunctionLookupContext(), false);
    }

    // annotate with Gandiva
    expr = expr.accept(annotator, null);

    Stopwatch javaCodeGenWatch = Stopwatch.createUnstarted();
    Stopwatch gandivaCodeGenWatch = Stopwatch.createUnstarted();

    ExpressionSplitter splitter = null;
    VectorContainer dataOut = null;
    final List<RecordBatchData> data = new ArrayList<>();
    try {
      // split the annotated expression
      OptionManager optionManager = testContext.getOptions();
      optionManager.setOption(OptionValue.createBoolean(
        OptionValue.OptionType.SYSTEM, ExecConstants.SPLIT_ENABLED.getOptionName(), true));
      ExpressionEvaluationOptions options = new ExpressionEvaluationOptions(optionManager);
      options.setCodeGenOption(SupportedEngines.CodeGenOption.Gandiva.toString());
      splitter = new ExpressionSplitter(context, vectorContainer, options, annotator, "_xxx");
      NamedExpression namedExpression = new NamedExpression(expr, new FieldReference("out"));
      // split the expression and set the splits up for execution
      dataOut = context.createOutputVectorContainer();
      splitter.addExpr(dataOut, namedExpression);
      vectorContainer = splitter.setupProjector(dataOut, javaCodeGenWatch, gandivaCodeGenWatch);

      // ExprToString is used to convert an expression to a string for comparison
      ExprToString stringBuilder = new ExprToString(vectorContainer);

      // verify number of splits
      List<ExpressionSplit> splits = splitter.getSplits();
      assertEquals(expSplits.length, splits.size());

      if (expSplits.length == 1) {
        // either executed in Java or Gandiva, no split mode
        if (expSplits[0].useGandiva) {
          // completely in Gandiva
          assertEquals(splitter.getNumExprsInJava(), 0);
          assertEquals(splitter.getNumExprsInGandiva(), 1);
        } else {
          // completely in Java
          assertEquals(splitter.getNumExprsInJava(), 1);
          assertEquals(splitter.getNumExprsInGandiva(), 0);
        }

        assertEquals(splitter.getNumExprsInBoth(), 0);
        assertEquals(splitter.getNumSplitsInBoth(), 0);
      } else {
        assertEquals(splitter.getNumExprsInJava(), 0);
        assertEquals(splitter.getNumExprsInGandiva(), 0);
        assertEquals(splitter.getNumExprsInBoth(), 1);
        assertEquals(splitter.getNumSplitsInBoth(), expSplits.length);
      }

      // verify each split with the expected value
      for (int i = 0; i < splits.size(); i++) {
        NamedExpression split = splits.get(i).getNamedExpression();

        String expExpr = expSplits[i].expr.replaceAll("\\s+", "").toLowerCase();
        String actualExpr = stringBuilder.expr2String(split.getExpr()).replaceAll("\\s+", "").toLowerCase();

        assertEquals(expSplits[i].useGandiva, splits.get(i).getExecutionEngine() == SupportedEngines
          .Engine.GANDIVA);
        if (i == splits.size() - 1) {
          assertEquals("out", split.getRef().getAsUnescapedPath());
        } else {
          assertEquals(expSplits[i].name, split.getRef().getAsUnescapedPath());
        }
        assertEquals(expExpr, actualExpr);

        // make sure that the dependencies and execution order are correct
        assertEquals(expSplits[i].execIteration, splits.get(i).getExecIteration());
        List<String> actualDependencies = splits.get(i).getDependencies();
        assertTrue(actualDependencies.containsAll(expSplits[i].dependencies));
        assertTrue(expSplits[i].dependencies.containsAll(actualDependencies));
      }

      // setup output vectors
      dataOut.buildSchema(BatchSchema.SelectionVectorMode.NONE);

      // Evaluate the expression
      int count;
      while ((count = generator.next(batchSize)) != 0) {
        // evaluate data in batches
        splitter.projectRecords(count, javaCodeGenWatch, gandivaCodeGenWatch);
        dataOut.setRecordCount(count);
        data.add(new RecordBatchData(dataOut, testAllocator));
      }

      // validate the output data
      output.checkValid(data);
    } finally {
      // cleanup
      if (dataOut != null) {
        dataOut.close();
      }
      generator.close();
      if (splitter != null) {
        splitter.close();
      }
      AutoCloseables.close(data);
    }
  }

  private void splitAndVerify(String query, Fixtures.Table input, Fixtures.Table output, Split[] expSplits, GandivaAnnotator annotator)
    throws Exception
  {
    splitAndVerify(toExpr(query), input, output, expSplits, annotator);
  }

  @Test
  public void noSplitIf() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract", "add");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "(if (greater_than(c0, c1)) then (subtract(c0, c1)) else (add(c1, c0)) end)", 1, 0)
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitCondJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("subtract", "add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitCondGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (add(c1, c0)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitThenJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "add");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (add(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (subtract(c0, c1)) else (_xxx1) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitThenGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("subtract");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (_xxx1) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitElseJava() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (subtract(c0, c1)) else (cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (_xxx1) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void splitElseGandiva() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (add(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (subtract(c0, c1)) else (_xxx1) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(ifQuery, ifInput, ifOutput, expSplits, annotator);
  }

  @Test
  public void testNestedIfInFunction() throws Exception {
    // if (c0 > c1) then (((c0 > 10) ? 10 : c0) + c0) else (c1 + c0)
    String query = "case when c0 > c1 then (case when c0 > 10 then 10i else c0 end) + c0 else c1 + c0 end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(10, 11),
      tr(4, 3),
      tr(11, 5)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(21),
      tr(8),
      tr(21)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("add");

    Split[] expSplits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, c1)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (greater_than(c0, 10i)) else(cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (add((if (_xxx1) then (10i) else (c0) end), c0)) else (add(c1, c0)) end)", 3, 0, "_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void testNestedIfInFunctionSplitThen() throws Exception {
    // if (c0 > c1) then (((c0 > 10) ? 10 : c0) + c0) else (c1 - c0)
    String query = "case when c0 > c1 then (case when c0 > 10 then 10i else c0 end) + c0 else c1 - c0 end";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(10, 11),
      tr(4, 3),
      tr(11, 5)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(1),
      tr(8),
      tr(21)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "subtract");

    Split[] expSplits = new Split[]{
      new Split(true, "_xxx0", "greater_than(c0, c1)", 1, 3),
      new Split(true, "_xxx1", "(if (_xxx0) then ((if (greater_than(c0, 10i)) then (10i) else (c0) end)) else(cast((__$internal_null$__) as int)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (cast((__$internal_null$__) as int)) else (subtract(c1, c0)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx3", "(if (_xxx0) then (add(_xxx1, c0)) else (_xxx2) end)", 3, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, expSplits, annotator);
  }

  @Test
  public void ifTimestamp() throws Exception {
    GandivaAnnotator annotator = new GandivaAnnotator("less_than", "greater_than", "booleanAnd");
    String query = "case when (c0 < castTIMESTAMP(698457600000l) AND (c1 > castTIMESTAMP(909619200000l))) then (castTIMESTAMP(1395822273000l))  else (castTIMESTAMP(1395822273999l)) end";
    Fixtures.Table input = Fixtures.t(
      th("c0", "c1"),
      tr(date("2001-01-01"), date("2001-12-31"))
    );

    Fixtures.Table output = Fixtures.t(
      th("out"),
      tr(ts("2014-03-26T08:24:33.999"))
    );

    Split[] splits = {
      new Split(false, "_xxx0", "casttimestamp(c0)", 1, 1),
      new Split(false, "_xxx1", "casttimestamp(698457600000l)", 1, 1),
      new Split(true, "_xxx2", "less_than(_xxx0, _xxx1)", 2, 3, "_xxx0", "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx2) then (casttimestamp(c1)) else (cast((__$internal_null$__) as timestamp)) end)", 3, 1, "_xxx2"),
      new Split(false, "_xxx4", "(if (_xxx2) then (casttimestamp(909619200000l)) else (cast((__$internal_null$__) as timestamp)) end)", 3, 1, "_xxx2"),
      new Split(true, "_xxx5", "(if (_xxx2) then(greater_than(_xxx3, _xxx4)) else (false) end)", 4, 1, "_xxx2", "_xxx3", "_xxx4"),
      new Split(false, "_xxx6", "(if (_xxx5) then(casttimestamp(1395822273000l)) else(casttimestamp(1395822273999l)) end)", 5, 0, "_xxx5")
    };
    splitAndVerify(query, input, output, splits, annotator);
  }

  private int evalFunction(int c0, int c1, int c2) {
    return (c0 + c1) * c2 + (c0 - c1);
  }

  private List<Fixtures.Table> genRandomDataForFunction(int numRows, int batchSize) {
    Fixtures.DataRow[] inputData = new Fixtures.DataRow[numRows];
    Fixtures.DataRow[] outputData = new Fixtures.DataRow[numRows];

    Random rand = new Random(0);
    for(int i = 0; i < numRows; i++) {
      int c0 = rand.nextInt(100);
      int c1 = rand.nextInt(100);
      int c2 = rand.nextInt(100);

      inputData[i] = tr(c0, c1, c2);
      outputData[i] = tr(evalFunction(c0, c1, c2));
    }

    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      batchSize,
      inputData
    );

    Fixtures.Table output = Fixtures.t(
      th("out"),
      outputData
    );

    return Lists.newArrayList(input, output);
  }

  @Test
  public void testFunctionArgs() throws Exception {
    String query = "((c0 + c1) * c2) + (c0 - c1)";
    int batchSize = 4*1000; // 4k rows
    int numRows = 1000 * 1000; // 1m rows

    List<Fixtures.Table> inout = genRandomDataForFunction(numRows, batchSize);
    Fixtures.Table input = inout.get(0);
    Fixtures.Table output = inout.get(1);

    GandivaAnnotator annotator = new GandivaAnnotator("add", "subtract");
    Split[] splits = {
      new Split(true, "_xxx0", "add(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "multiply(_xxx0, c2)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "add(_xxx1, subtract(c0, c1))", 3, 0, "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);

    // support all functions and test
    annotator = new GandivaAnnotator("add", "subtract", "multiply");
    splits = new Split[]{
      new Split(true, "_xxx0", "add(multiply(add(c0, c1), c2), subtract(c0, c1))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testBooleanAnd() throws Exception {
    String query = "c0 > 10 AND c0 < 20";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18),
      tr(9),
      tr(24)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 20i)) else (false) end)", 2, 0,"_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("less_than");
    splits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 20i)) else (false) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("greater_than", "less_than", "booleanAnd");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanAnd(greater_than(c0, 10i), less_than(c0, 20i))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndBCGandiva() throws Exception {
    // A && B && C with B and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0), // A is false
      tr(28, 0), // B is false
      tr(20, 10) // C is false
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("less_than", "booleanAnd", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then ((if (less_than(c0, 24i)) then(equal(c1, 0i)) else (false) end)) else (false) end)", 2, 0,"_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndACGandiva() throws Exception {
    // A && B && C with A and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0),
      tr(28, 0),
      tr(20, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "booleanAnd", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (false) end)) else (false) end)", 3, 0,"_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3AndABGandiva() throws Exception {
    // A && B && C with A and B supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(18, 0),
      tr(9, 0),
      tr(28, 0),
      tr(20, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than", "booleanAnd", "less_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then ((if (_xxx1) then (equal(c1, 0i)) else (false) end)) else (false) end)", 3, 0,"_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4AndBDGandiva() throws Exception {
    // A && B && C && D with B and D supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0 AND c2 < 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 0, 1),
      tr(9, 0, 3), // false because of A
      tr(30, 0, 2), // false because of B
      tr(18, 1, 2), // false because of C
      tr(20, 10, 3) // false because of D
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "less_than", "mod");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 2, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx1) then (equal(c1, 0i)) else (cast((__$internal_null$__) as bit)) end)", 3, 1, "_xxx1"),
      new Split(true, "_xxx3", "(if (_xxx0) then ((if (_xxx1) then ((if (_xxx2) then (less_than(c2, 4i)) else (false) end)) else (false) end)) else (false) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4AndACGandiva() throws Exception {
    // A && B && C && D with A and C supported in Gandiva
    String query = "c0 > 10 AND c0 < 24 AND c1 == 0 AND c2 < 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 0, 1),
      tr(9, 0, 3), // false because of A
      tr(30, 0, 2), // false because of B
      tr(18, 1, 2), // false because of C
      tr(20, 10, 3) // false because of D
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(false),
      tr(false),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "greater_than", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (less_than(c0, 24i)) else (cast((__$internal_null$__) as bit)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx1) then (equal(c1, 0i)) else (cast((__$internal_null$__) as bit)) end)", 3, 1, "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then ((if (_xxx1) then ((if (_xxx2) then (less_than(c2, 4i)) else (false) end)) else (false) end)) else (false) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testBooleanOr() throws Exception {
    String query = "c0 > 10 OR c0 < 0";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18),
      tr(9),
      tr(24)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(true)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(false, "_xxx1", "(if (_xxx0) then (true) else (less_than(c0, 0i)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("less_than");
    splits = new Split[]{
      new Split(false, "_xxx0", "greater_than(c0, 10i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (true) else (less_than(c0, 0i)) end)", 2, 0, "_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("less_than", "greater_than", "booleanOr");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanOr(greater_than(c0, 10i), less_than(c0, 0i))", 1, 0),
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrBCGandiva() throws Exception {
    // A || B || C with B and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "less_than", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 24i)", 1, 1),
      new Split(true, "_xxx1", "(if (_xxx0) then (true) else ((if (less_than(c0, 10i)) then (true) else(equal(c1, 0i)) end)) end)", 2, 0,"_xxx0")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrACGandiva() throws Exception {
    // A || B || C with A and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than", "equal");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 1, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else (equal(c1, 0i)) end)) end)", 3, 0,"_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test3OrABGandiva() throws Exception {
    // A || B || C with A and B supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1"),
      2,
      tr(30, 1), // true due to A
      tr(9, 2), // true due to B
      tr(20, 0), // true due to C
      tr(24, 10)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than", "less_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 1, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else (equal(c1, 0i)) end)) end)", 3, 0,"_xxx0", "_xxx1")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4OrBCGandiva() throws Exception {
    // A || B || C || D with B and C supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0 OR c2 > 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(30, 1, 3), // true due to A
      tr(9, 2, -2), // true due to B
      tr(20, 0, 0), // true due to C
      tr(20, 3, 6), // true due to D
      tr(24, 10, 4)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "less_than", "equal");
    Split[] splits = {
      new Split(false, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(true, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 2, "_xxx0"),
      new Split(true, "_xxx2", "(if (_xxx1) then (cast((__$internal_null$__) as bit)) else (equal(c1, 0i)) end)", 3, 1, "_xxx1"),
      new Split(false, "_xxx3", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else ((if (_xxx2) then (true) else (greater_than(c2, 4i)) end)) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void test4OrADGandiva() throws Exception {
    // A || B || C || D with A and D supported in Gandiva
    String query = "c0 > 24 OR c0 < 10 OR c1 == 0 OR c2 > 4";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(30, 1, 3), // true due to A
      tr(9, 2, -2), // true due to B
      tr(20, 0, 0), // true due to C
      tr(20, 3, 6), // true due to D
      tr(24, 10, 4)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(true),
      tr(true),
      tr(true),
      tr(false)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanOr", "greater_than");
    Split[] splits = {
      new Split(true, "_xxx0", "greater_than(c0, 24i)", 1, 2),
      new Split(false, "_xxx1", "(if (_xxx0) then (cast((__$internal_null$__) as bit)) else (less_than(c0, 10i)) end)", 2, 2, "_xxx0"),
      new Split(false, "_xxx2", "(if (_xxx1) then (cast((__$internal_null$__) as bit)) else (equal(c1, 0i)) end)", 3, 1, "_xxx1"),
      new Split(true, "_xxx3", "(if (_xxx0) then (true) else ((if (_xxx1) then (true) else ((if (_xxx2) then (true) else (greater_than(c2, 4i)) end)) end)) end)", 4, 0, "_xxx0", "_xxx1", "_xxx2")
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testBooleanOperator() throws Exception {
    String query = "(c0 > 10 AND c0 < 20) OR (mod(c0, 2) == 0)";
    Fixtures.Table input = Fixtures.split(
      th("c0"),
      2,
      tr(18L),
      tr(9L),
      tr(24L)
    );

    Fixtures.Table output = t(
      th("out"),
      tr(true),
      tr(false),
      tr(true)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("booleanAnd", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    // or is not supported, but both the args are completely supported
    // or gets translated to if-expr
    Split[] splits = {
      new Split(true, "_xxx0", "(if (booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i)))) then (true) else(equal(mod(c0, 2i),0i)) end)", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);

    annotator = new GandivaAnnotator("booleanOr", "booleanAnd", "greater_than", "less_than", "mod", "equal", "castBIGINT");
    splits = new Split[]{
      new Split(true, "_xxx0", "booleanOr(booleanAnd(greater_than(c0, castBIGINT(10i)),less_than(c0, castBIGINT(20i))), equal(mod(c0, 2i),0i))", 1, 0)
    };

    splitAndVerify(query, input, output, splits, annotator);
  }

  @Test
  public void testExceptionHandling() throws Exception {
    String query = "(c0 / c1) + c2";
    Fixtures.Table input = Fixtures.split(
      th("c0", "c1", "c2"),
      2,
      tr(18, 2, 5),
      tr(20, 5, 1),
      tr(20, 0, 4) // this causes a divide by zero exception during evaluation
    );

    Fixtures.Table output = t(
      th("out"),
      tr(14),
      tr(5),
      tr(4)
    );

    GandivaAnnotator annotator = new GandivaAnnotator("divide");
    Split[] splits = {
      new Split(true, "_xxx0", "divide(c0, c1)", 1, 1),
      new Split(false, "_xxx1", "add(_xxx0, c2)", 2, 0, "_xxx0")
    };

    boolean gotException = false;
    try {
      splitAndVerify(query, input, output, splits, annotator);
    } catch (Exception e) {
      // suppress exception
      gotException = true;
    }

    // The post-test checks ensure that there is no memory leak in the ArrowBufs allocated
    assertTrue(gotException);
  }

  // Converts an expression to a tree
  // Uses the schema to convert reads to column names
  private class ExprToString extends ExpressionStringBuilder {
    VectorAccessible incoming;

    ExprToString(VectorAccessible incoming) {
      super();
      this.incoming = incoming;
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

        VectorWrapper wrapper = this.incoming.getValueAccessorById(FieldVector.class, readExpression.getFieldId().getFieldIds());
        sb.append(wrapper.getField().getName());
      }

      return null;
    }
  }

  // Custom visitor to annotate an expression with Gandiva support
  // Takes a list of supported functions
  private class GandivaAnnotator extends AbstractExprVisitor<CodeGenContext, Void, GandivaException> implements ExpressionSplitHelper {
    // List of functions supported by Gandiva
    private final List<String> supportedFunctions;
    // List of functions that can be tree-roots
    private final List<String> rootFunctions;

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

    boolean allExpressionsSupported(List<LogicalExpression> elements) {
      for(LogicalExpression exp : elements) {
        CodeGenContext context = (CodeGenContext) exp;
        if (!context.isSubExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression holder, Void
      value) throws GandivaException {
      List<LogicalExpression> argsContext = Lists.newArrayList();
      for(LogicalExpression e : holder.args) {
        CodeGenContext context = e.accept(this, null);
        argsContext.add(context);
      }

      FunctionHolderExpression newFunction;
      if (holder.getHolder() instanceof BaseFunctionHolder) {
        newFunction = new FunctionHolderExpr(holder.getName(), (BaseFunctionHolder) holder.getHolder(), argsContext);
      } else {
        newFunction = new GandivaFunctionHolderExpression(holder.getName(), (GandivaFunctionHolder)holder.getHolder(), argsContext);
      }

      CodeGenContext functionContext = new CodeGenContext(newFunction);
      if (this.supportedFunctions.contains(holder.getName().toLowerCase())) {
        functionContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        if (allExpressionsSupported(argsContext)) {
          functionContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
        }
      }

      return functionContext;
    }

    @Override
    public CodeGenContext visitIfExpression(IfExpression ifExpr, Void value) throws GandivaException {
      CodeGenContext ifConditionContext = ifExpr.ifCondition.condition.accept(this, null);
      CodeGenContext thenExprContext = ifExpr.ifCondition.expression.accept(this, null);
      CodeGenContext elseExprContext = ifExpr.elseExpression.accept(this, null);

      IfExpression newIfExpr = IfExpression.newBuilder().setIfCondition(new IfExpression
        .IfCondition(ifConditionContext, thenExprContext)).setElse(elseExprContext).build();
      CodeGenContext ifExprContext = new CodeGenContext(newIfExpr);
      ifExprContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      if (allExpressionsSupported(Lists.newArrayList(ifConditionContext, thenExprContext, elseExprContext))) {
        ifExprContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      }

      return ifExprContext;
    }

    @Override
    public CodeGenContext visitBooleanOperator(BooleanOperator op, Void value) throws GandivaException {
      List<LogicalExpression> argsContext = Lists.newArrayList();
      for(LogicalExpression e : op.args) {
        CodeGenContext context = e.accept(this, null);
        argsContext.add(context);
      }
      BooleanOperator newOperator = new BooleanOperator(op.getName(), argsContext);
      CodeGenContext booleanOpContext = new CodeGenContext(newOperator);
      if (this.supportedFunctions.contains(op.getName().toLowerCase())) {
        booleanOpContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        if (allExpressionsSupported(argsContext)) {
          booleanOpContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
        }
      }

      return booleanOpContext;
    }

    CodeGenContext annotateWithGandivaSupport(LogicalExpression e) {
      CodeGenContext context = new CodeGenContext(e);
      context.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      context.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      return context;
    }

    @Override
    public CodeGenContext visitUnknown(LogicalExpression e, Void value) {
      if (e instanceof ValueVectorReadExpression) {
        // let Gandiva support all reads
        return annotateWithGandivaSupport(e);
      }
      return null;
    }

    @Override
    public CodeGenContext visitFloatConstant(ValueExpressions.FloatExpression fExpr, Void value) {
      return annotateWithGandivaSupport(fExpr);
    }

    @Override
    public CodeGenContext visitIntConstant(ValueExpressions.IntExpression intExpr, Void value) {
      return annotateWithGandivaSupport(intExpr);
    }

    @Override
    public CodeGenContext visitLongConstant(ValueExpressions.LongExpression intExpr, Void value) {
      return annotateWithGandivaSupport(intExpr);
    }

    @Override
    public CodeGenContext visitDateConstant(ValueExpressions.DateExpression intExpr, Void value) {
      return annotateWithGandivaSupport(intExpr);
    }

    @Override
    public CodeGenContext visitTimeConstant(ValueExpressions.TimeExpression intExpr, Void value) {
      return annotateWithGandivaSupport(intExpr);
    }

    @Override
    public CodeGenContext visitTimeStampConstant(ValueExpressions.TimeStampExpression intExpr, Void value) {
      return annotateWithGandivaSupport(intExpr);
    }

    @Override
    public CodeGenContext visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Void value) {
      return annotateWithGandivaSupport(dExpr);
    }

    @Override
    public CodeGenContext visitBooleanConstant(ValueExpressions.BooleanExpression e, Void value) {
      return annotateWithGandivaSupport(e);
    }

    @Override
    public CodeGenContext visitQuotedStringConstant(ValueExpressions.QuotedString e, Void value) {
      return annotateWithGandivaSupport(e);
    }
  }
}
