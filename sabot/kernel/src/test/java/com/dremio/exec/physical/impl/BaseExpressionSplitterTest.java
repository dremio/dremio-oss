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
package com.dremio.exec.physical.impl;

import static com.dremio.sabot.Fixtures.th;
import static com.dremio.sabot.Fixtures.tr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.common.AutoCloseables;
import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.ErrorCollector;
import com.dremio.common.expression.ErrorCollectorImpl;
import com.dremio.common.expression.ExpressionStringBuilder;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
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
import com.dremio.exec.record.RecordBatchData;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.sabot.BaseTestFunction;
import com.dremio.sabot.Fixtures;
import com.dremio.sabot.Generator;
import com.dremio.sabot.exec.context.OperatorContextImpl;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.junit.Before;
import org.junit.BeforeClass;

public class BaseExpressionSplitterTest extends BaseTestFunction {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BaseExpressionSplitterTest.class);

  @Before
  public void cleanExpToExpSplitCache() {
    testContext.invalidateExpToExpSplitsCache();
  }

  @BeforeClass
  public static void setExpSplitCacheEnabledToFalse() {
    testContext
        .getOptions()
        .setOption(
            OptionValue.createBoolean(
                OptionValue.OptionType.SYSTEM, ExecConstants.SPLIT_CACHING_ENABLED_KEY, false));
  }

  /**
   * Split the query and match the splits against expected splits.
   *
   * <p>Splits the expression. First, the expression is materialized (using Java only) Second, the
   * expression is annotated using a custom annotator for Gandiva support Third, the expression is
   * split and the splits verified Fourth, the split expression evaluated and the output value
   * verified
   *
   * @param expr expression to split
   * @param input input to pass to the expression
   * @param output expected output
   * @param expSplits expected splits
   * @param annotator Gandiva supported functions
   * @throws Exception on error
   */
  protected void splitAndVerify(
      LogicalExpression expr,
      Fixtures.Table input,
      Fixtures.Table output,
      Split[] expSplits,
      GandivaAnnotator annotator)
      throws Exception {
    // Get the allocator for allocating vectors
    BufferAllocator testAllocator = getTestAllocator();
    Generator generator = input.toGenerator(testAllocator);
    // find the schema for the expression
    VectorAccessible vectorContainer = generator.getOutput();

    Project pop =
        new Project(
            OpProps.prototype(),
            null,
            Collections.singletonList(new NamedExpression(expr, new FieldReference("out"))));
    final BufferAllocator childAllocator =
        testAllocator.newChildAllocator(
            pop.getClass().getSimpleName(),
            pop.getProps().getMemReserve(),
            pop.getProps().getMemLimit() == 0 ? Long.MAX_VALUE : pop.getProps().getMemLimit());

    int batchSize = 1;
    final OperatorContextImpl context =
        testContext.getNewOperatorContext(childAllocator, pop, batchSize);
    testCloseables.add(context);

    // materialize expression
    try (ErrorCollector errorCollector = new ErrorCollectorImpl()) {
      expr =
          ExpressionTreeMaterializer.materialize(
              expr,
              vectorContainer.getSchema(),
              errorCollector,
              testContext.getFunctionLookupContext(),
              false);
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
      optionManager.setOption(
          OptionValue.createBoolean(
              OptionValue.OptionType.SYSTEM, ExecConstants.SPLIT_ENABLED.getOptionName(), true));
      ExpressionEvaluationOptions options = new ExpressionEvaluationOptions(optionManager);
      options.setCodeGenOption(SupportedEngines.CodeGenOption.Gandiva.toString());
      splitter = new ExpressionSplitter(context, vectorContainer, options, annotator, "_xxx", true);
      NamedExpression namedExpression = new NamedExpression(expr, new FieldReference("out"));
      // split the expression and set the splits up for execution
      dataOut = context.createOutputVectorContainer();
      splitter.addExpr(dataOut, namedExpression);
      vectorContainer = splitter.setupProjector(dataOut, javaCodeGenWatch, gandivaCodeGenWatch);

      // ExprToString is used to convert an expression to a string for comparison
      ExprToString stringBuilder = new ExprToString(vectorContainer);

      // verify number of splits
      List<ExpressionSplit> splits = splitter.getSplits();
      printSplits(splits, stringBuilder);
      if (expSplits != null) {
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
          String actualExpr =
              stringBuilder.expr2String(split.getExpr()).replaceAll("\\s+", "").toLowerCase();

          assertEquals(
              expSplits[i].useGandiva,
              splits.get(i).getExecutionEngine() == SupportedEngines.Engine.GANDIVA);
          if (i == splits.size() - 1) {
            assertEquals("out", split.getRef().getAsUnescapedPath());
          } else {
            assertEquals(expSplits[i].name, split.getRef().getAsUnescapedPath());
          }
          assertEquals(expExpr, actualExpr);

          // make sure that the dependencies and execution order are correct
          assertEquals(expSplits[i].execIteration, splits.get(i).getExecIteration());
          assertEquals(expSplits[i].numReaders, splits.get(i).getTotalReadersOfOutput());
          List<String> actualDependencies = splits.get(i).getDependencies();
          assertTrue(actualDependencies.containsAll(expSplits[i].dependencies));
          assertTrue(expSplits[i].dependencies.containsAll(actualDependencies));
        }
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

  protected void splitAndVerify(
      String query,
      Fixtures.Table input,
      Fixtures.Table output,
      Split[] expSplits,
      GandivaAnnotator annotator)
      throws Exception {
    splitAndVerify(toExpr(query), input, output, expSplits, annotator);
  }

  protected void splitAndVerifyCase(
      String query,
      Fixtures.Table input,
      Fixtures.Table output,
      Split[] expSplits,
      GandivaAnnotator annotator)
      throws Exception {
    splitAndVerify(toExprCase(query), input, output, expSplits, annotator);
  }

  protected List<Fixtures.Table> genRandomDataForFunction(int numRows, int batchSize) {
    Fixtures.DataRow[] inputData = new Fixtures.DataRow[numRows];
    Fixtures.DataRow[] outputData = new Fixtures.DataRow[numRows];

    Random rand = new Random(0);
    for (int i = 0; i < numRows; i++) {
      int c0 = rand.nextInt(100);
      int c1 = rand.nextInt(100);
      int c2 = rand.nextInt(100);

      inputData[i] = tr(c0, c1, c2);
      outputData[i] = tr(evalFunction(c0, c1, c2));
    }

    Fixtures.Table input = Fixtures.split(th("c0", "c1", "c2"), batchSize, inputData);

    Fixtures.Table output = Fixtures.t(th("out"), outputData);

    return Lists.newArrayList(input, output);
  }

  private int evalFunction(int c0, int c1, int c2) {
    return (c0 + c1) * c2 + (c0 - c1);
  }

  private void printSplits(List<ExpressionSplit> splits, ExprToString stringBuilder) {
    if (logger.isTraceEnabled()) {
      splits.forEach(
          (s) -> {
            final NamedExpression split = s.getNamedExpression();
            final String actualExpr = stringBuilder.expr2String(split.getExpr());
            final String dependencies = String.join(",", s.getDependencies());
            System.out.printf(
                "%s:%s:%s:%s:%d:%d%n",
                s.getOutputName(),
                s.getExecutionEngine(),
                actualExpr,
                dependencies,
                s.getExecIteration(),
                s.getTotalReadersOfOutput());
          });
    }
  }

  /** Private class to verify the output of the split */
  protected static class Split {
    boolean useGandiva;
    String name;
    String expr;
    int execIteration;
    int numReaders;
    List<String> dependencies;

    Split(
        boolean useGandiva,
        String name,
        String expr,
        int iteration,
        int numReaders,
        String... dependencies) {
      this.useGandiva = useGandiva;
      this.name = name;
      this.expr = expr;
      this.execIteration = iteration;
      this.numReaders = numReaders;
      this.dependencies = Arrays.asList(dependencies);
    }
  }

  // Converts an expression to a tree
  // Uses the schema to convert reads to column names
  private static class ExprToString extends ExpressionStringBuilder {
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
        ValueVectorReadExpression readExpression = (ValueVectorReadExpression) e;

        VectorWrapper<?> wrapper =
            this.incoming.getValueAccessorById(
                FieldVector.class, readExpression.getFieldId().getFieldIds());
        sb.append(wrapper.getField().getName());
      }

      return null;
    }
  }

  /**
   * Custom visitor to annotate an expression with Gandiva support. Takes a list of supported
   * functions.
   */
  protected static class GandivaAnnotator
      extends AbstractExprVisitor<CodeGenContext, Void, GandivaException>
      implements ExpressionSplitHelper {
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
      for (Object o : inList) {
        String fnName = (String) o;
        stringList.add(fnName.toLowerCase());
      }
    }

    void setRootFunctions(String... rootFunctions) {
      List<Object> functions = Arrays.asList(rootFunctions);
      copyStringsTo(this.rootFunctions, functions);
    }

    void addFunctions(String... functions) {
      copyStringsTo(this.supportedFunctions, Arrays.asList(functions));
    }

    @Override
    public boolean canSplitAt(LogicalExpression e) {
      String fnName = null;
      if (e instanceof FunctionHolderExpression) {
        fnName = ((FunctionHolderExpression) e).getName();
      }

      if (e instanceof BooleanOperator) {
        fnName = ((BooleanOperator) e).getName();
      }

      if (fnName == null) {
        return true;
      }

      // Dont allow split if the tree root is one of the functions in rootFunctions
      return (!rootFunctions.contains(fnName.toLowerCase()));
    }

    boolean allExpressionsSupported(List<LogicalExpression> elements) {
      for (LogicalExpression exp : elements) {
        CodeGenContext context = (CodeGenContext) exp;
        if (!context.isSubExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
          return false;
        }
      }
      return true;
    }

    boolean anyExpressionsSupported(List<LogicalExpression> elements) {
      for (LogicalExpression exp : elements) {
        CodeGenContext context = (CodeGenContext) exp;
        if (context.isSubExpressionExecutableInEngine(SupportedEngines.Engine.GANDIVA)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public CodeGenContext visitFunctionHolderExpression(FunctionHolderExpression holder, Void value)
        throws GandivaException {
      List<LogicalExpression> argsContext = Lists.newArrayList();
      for (LogicalExpression e : holder.args) {
        CodeGenContext context = e.accept(this, null);
        argsContext.add(context);
      }

      FunctionHolderExpression newFunction;
      if (holder.getHolder() instanceof BaseFunctionHolder) {
        newFunction =
            new FunctionHolderExpr(
                holder.getName(), (BaseFunctionHolder) holder.getHolder(), argsContext);
      } else {
        newFunction =
            new GandivaFunctionHolderExpression(
                holder.getName(), (GandivaFunctionHolder) holder.getHolder(), argsContext);
      }

      CodeGenContext functionContext = new CodeGenContext(newFunction);
      if (this.supportedFunctions.contains(holder.getName().toLowerCase())) {
        functionContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        if (allExpressionsSupported(argsContext)) {
          functionContext.addSupportedExecutionEngineForSubExpression(
              SupportedEngines.Engine.GANDIVA);
        }
      }

      return functionContext;
    }

    @Override
    public CodeGenContext visitIfExpression(IfExpression ifExpr, Void value)
        throws GandivaException {
      CodeGenContext ifConditionContext = ifExpr.ifCondition.condition.accept(this, null);
      CodeGenContext thenExprContext = ifExpr.ifCondition.expression.accept(this, null);
      CodeGenContext elseExprContext = ifExpr.elseExpression.accept(this, null);

      IfExpression newIfExpr =
          IfExpression.newBuilder()
              .setIfCondition(new IfExpression.IfCondition(ifConditionContext, thenExprContext))
              .setElse(elseExprContext)
              .build();
      CodeGenContext ifExprContext = new CodeGenContext(newIfExpr);
      final List<LogicalExpression> expressions =
          Arrays.asList(ifConditionContext, thenExprContext, elseExprContext);
      if (anyExpressionsSupported(expressions)) {
        ifExprContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      }
      if (allExpressionsSupported(expressions)) {
        ifExprContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      }
      return ifExprContext;
    }

    @Override
    public CodeGenContext visitCaseExpression(CaseExpression caseExpression, Void value)
        throws GandivaException {
      List<LogicalExpression> expressions = new ArrayList<>();
      List<CaseExpression.CaseConditionNode> caseConditions = new ArrayList<>();
      for (CaseExpression.CaseConditionNode conditionNode : caseExpression.caseConditions) {
        LogicalExpression whenExpr = conditionNode.whenExpr.accept(this, value);
        LogicalExpression thenExpr = conditionNode.thenExpr.accept(this, value);
        CaseExpression.CaseConditionNode newConditionExpression =
            new CaseExpression.CaseConditionNode(whenExpr, thenExpr);
        caseConditions.add(newConditionExpression);
        expressions.add(whenExpr);
        expressions.add(thenExpr);
      }
      CodeGenContext elseExpr = caseExpression.elseExpr.accept(this, value);
      expressions.add(elseExpr);
      CaseExpression result =
          CaseExpression.newBuilder()
              .setCaseConditions(caseConditions)
              .setElseExpr(elseExpr)
              .build();
      CodeGenContext caseExpr = new CodeGenContext(result);
      if (anyExpressionsSupported(expressions)) {
        caseExpr.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
      }
      if (allExpressionsSupported(expressions)) {
        caseExpr.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
      }
      return caseExpr;
    }

    @Override
    public CodeGenContext visitBooleanOperator(BooleanOperator op, Void value)
        throws GandivaException {
      List<LogicalExpression> argsContext = Lists.newArrayList();
      for (LogicalExpression e : op.args) {
        CodeGenContext context = e.accept(this, null);
        argsContext.add(context);
      }
      BooleanOperator newOperator = new BooleanOperator(op.getName(), argsContext);
      CodeGenContext booleanOpContext = new CodeGenContext(newOperator);
      if (this.supportedFunctions.contains(op.getName().toLowerCase())) {
        booleanOpContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
        if (allExpressionsSupported(argsContext)) {
          booleanOpContext.addSupportedExecutionEngineForSubExpression(
              SupportedEngines.Engine.GANDIVA);
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
    public CodeGenContext visitTimeStampConstant(
        ValueExpressions.TimeStampExpression intExpr, Void value) {
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

    @Override
    public CodeGenContext visitNullConstant(TypedNullConstant e, Void value) {
      return annotateWithGandivaSupport(e);
    }
  }

  protected static final class GandivaAnnotatorCase extends GandivaAnnotator {
    GandivaAnnotatorCase(String... supportedFunctions) {
      super(supportedFunctions);
      addFunctions("equal", "greater_than", "less_than");
    }
  }
}
