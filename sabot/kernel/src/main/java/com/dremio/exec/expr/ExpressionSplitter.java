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
package com.dremio.exec.expr;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.arrow.gandiva.exceptions.GandivaException;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.pojo.Field;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FieldReference;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SchemaPath;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorContainer;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.llvm.expr.GandivaPushdownSieve;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

/**
 * Splits expressions, sets up the pipeline to evaluate the splits.
 */
public class ExpressionSplitter implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionSplitter.class);
  private static final String DEFAULT_TMP_OUTPUT_NAME = "_split_expr";

  private static AtomicLong SPLIT_INSTANCE_COUNTER = new AtomicLong(0);

  // The various splits in the expression
  final List<ExpressionSplit> splitExpressions;

  // The original VectorAccessible structure for the expression that's being split
  final VectorAccessible incoming;

  // The modified VectorContainer containing the original VectorAccessible structure
  // and the elements for the intermediate tree-roots
  final VectorContainer vectorContainer;

  // Helper to decide if an intermediate node can be split and made tree-root
  final ExpressionSplitHelper gandivaSplitHelper;

  // Operator context. Need this for the allocator to allocate space for the intermediate tree-roots
  final OperatorContext context;

  // Prefix for the names of the intermediate nodes that are now tree-roots
  final String outputFieldPrefix;
  int outputFieldCounter = 0;

  // execution pipeline
  final List<SplitStageExecutor> execPipeline;

  // materializer options
  final ExpressionEvaluationOptions options;

  // is split enabled
  final boolean isSplitEnabled;

  // code generation option
  final SupportedEngines.CodeGenOption codeGenOption;

  // preferred execution type
  final SupportedEngines.Engine preferredEngine;
  // the other execution type
  final SupportedEngines.Engine nonPreferredEngine;

  // Splitter instance counter. This is included in all log messages to make debugging easy
  final long instanceCounter;

  public ExpressionSplitter(OperatorContext context, VectorAccessible incoming, ExpressionEvaluationOptions options) {
    this(context, incoming, options, new GandivaPushdownSieve(), ExpressionSplitter.DEFAULT_TMP_OUTPUT_NAME);
  }

  public ExpressionSplitter(OperatorContext context, VectorAccessible incoming, ExpressionEvaluationOptions options, ExpressionSplitHelper gandivaSplitHelper, String outputPrefix) {
    this.context = context;
    this.options = options;
    this.gandivaSplitHelper = gandivaSplitHelper;
    this.outputFieldPrefix = outputPrefix;
    this.splitExpressions = Lists.newArrayList();
    this.incoming = incoming;

    // Add all ValueVectors from incoming to vector
    this.vectorContainer = new VectorContainer(context.getAllocator());
    for (VectorWrapper wrapper : incoming) {
      this.vectorContainer.add(wrapper.getValueVector());
    }

    this.execPipeline = Lists.newArrayList();
    this.isSplitEnabled = options.isSplitEnabled();
    this.instanceCounter = SPLIT_INSTANCE_COUNTER.incrementAndGet();

    this.codeGenOption = options.getCodeGenOption();
    switch (this.codeGenOption) {
      case Gandiva:
      case GandivaOnly:
        this.preferredEngine = SupportedEngines.Engine.GANDIVA;
        this.nonPreferredEngine = SupportedEngines.Engine.JAVA;
        break;
      case Java:
      default:
        this.preferredEngine = SupportedEngines.Engine.JAVA;
        this.nonPreferredEngine = SupportedEngines.Engine.GANDIVA;
        break;
    }
  }

  void log(String s, Object... objects) {
    logger.debug("Expression {}: " + s, instanceCounter, objects);
  }

  // Splits the given expression
  private ExpressionSplit splitExpression(NamedExpression namedExpression) throws Exception {
    SupportedEngines executionEngine = new SupportedEngines();
    CodeGenContext expr = (CodeGenContext) namedExpression.getExpr();
    SplitDependencyTracker myTracker = new SplitDependencyTracker(expr.getExecutionEngineForExpression(), null, true);
    NamedExpression newExpr = namedExpression;
    int numSplitsBefore = this.splitExpressions.size();
    if (isSplitEnabled) {
      PreferenceBasedSplitter preferenceBasedSplitter = new PreferenceBasedSplitter(this, this
        .preferredEngine, this.nonPreferredEngine);
      CodeGenContext e = expr.accept(preferenceBasedSplitter, myTracker);
      newExpr = new NamedExpression(e, namedExpression.getRef());
      executionEngine = e.getExecutionEngineForSubExpression();
    } else {
      // if splits are not enabled, execute a single split (the entire expression) in Java.
      executionEngine.add(SupportedEngines.Engine.JAVA);
    }

    SupportedEngines.Engine engineForSplit = executionEngine.contains(this
      .preferredEngine) ? this.preferredEngine : this.nonPreferredEngine;
    ExpressionSplit split = new ExpressionSplit(newExpr, myTracker, null, null, true,
      engineForSplit);
    this.splitExpressions.add(split);

    int numSplits = this.splitExpressions.size() - numSplitsBefore;
    if (numSplits == 1) {
      LogicalExpression finalExpr = split.getNamedExpression().getExpr();
      if (split.getExecutionEngine() == SupportedEngines.Engine.GANDIVA) {
        log("Expression executed entirely in Gandiva {}", finalExpr);
      } else {
        log("Expression executed entirely in Java {}", finalExpr);
      }
    } else {
      // more than one split
      // For debugging, print all splits
      log("Mixed mode execution for expression {}", namedExpression.getExpr());
      for(int i = numSplitsBefore; i < numSplits; i++) {
        ExpressionSplit curSplit = this.splitExpressions.get(i);
        if (curSplit.getExecutionEngine()== SupportedEngines.Engine.GANDIVA) {
          log("Split {} evaluated in Gandiva = {}", (i + 1 - numSplitsBefore), curSplit.getNamedExpression().getExpr());
        } else {
          log("Split {} evaluated in Java = {}", (i + 1 - numSplitsBefore), curSplit.getNamedExpression().getExpr());
        }
      }
    }

    // Build the schema for the combined schema
    vectorContainer.buildSchema(BatchSchema.SelectionVectorMode.NONE);
    return split;
  }

  // iterate over the splits and find out when they can execute
  private void createPipeline() {
    List<ExpressionSplit> pendingSplits = Lists.newArrayList();
    pendingSplits.addAll(splitExpressions);
    List<String> doneSplits = Lists.newArrayList();
    int execIteration = 1;
    while (!pendingSplits.isEmpty()) {
      Iterator<ExpressionSplit> iterator = pendingSplits.iterator();
      List<String> doneInThisIteration = Lists.newArrayList();
      SplitStageExecutor splitStageExecutor = new SplitStageExecutor(context, vectorContainer, preferredEngine, instanceCounter);

      while (iterator.hasNext()) {
        ExpressionSplit split = iterator.next();

        if (doneSplits.containsAll(split.getDependencies())) {
          split.setExecIteration(execIteration);
          iterator.remove();

          if (!split.isOriginalExpression()) {
            // The name for outputs of the actual expressions is
            // not generated by the splitter, and should not be
            // considered a dependency that has been evaluated
            doneInThisIteration.add(split.getOutputName());
          }

          splitStageExecutor.addSplit(split);
        }
      }

      execIteration++;
      doneSplits.addAll(doneInThisIteration);
      execPipeline.add(splitStageExecutor);
    }
  }

  // setup the pipeline for project operations
  private void projectorSetup(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException {
    for(SplitStageExecutor splitStageExecutor : execPipeline) {
      splitStageExecutor.setupProjector(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);
    }
  }

  // setup the pipeline for filter operations
  private void filterSetup(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException, Exception {
    for(SplitStageExecutor splitStageExecutor : execPipeline) {
      splitStageExecutor.setupFilter(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);
    }
  }

  public List<ExpressionSplit> getSplits() {
    return this.splitExpressions;
  }

  // Add one expression to be split
  public ValueVector addExpr(VectorContainer outgoing, NamedExpression namedExpression) throws Exception {
    ExpressionSplit split = splitExpression(namedExpression);
    LogicalExpression expr = split.getNamedExpression().getExpr();
    Field outputField = expr.getCompleteType().toField(namedExpression.getRef());
    return outgoing.addOrGet(outputField);
  }

  private void verifySplitsInGandiva() throws Exception {
    if (codeGenOption != SupportedEngines.CodeGenOption.GandivaOnly) {
      return;
    }

    // ensure that all splits can execute in Gandiva
    for(ExpressionSplit split : splitExpressions) {

      if (split.getExecutionEngine() != SupportedEngines.Engine.GANDIVA) {
        logger.error("CodeGenMode is GandivaOnly, Split {} cannot be executed in Gandiva", split
          .getNamedExpression().getExpr());
        throw new Exception("Expression cannot be executed entirely in Gandiva in GandivaOnly codeGen mode");
      }
    }
  }

  // create and setup the pipeline for project operations
  public VectorContainer setupProjector(VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch)
    throws Exception {
    verifySplitsInGandiva();
    createPipeline();
    projectorSetup(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);

    return vectorContainer;
  }

  // create and setup the pipeline for filter operation
  public void setupFilter(LogicalExpression expr, VectorContainer outgoing, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws GandivaException, Exception {
    splitExpression(new NamedExpression(expr, new FieldReference("_filter_")));
    verifySplitsInGandiva();
    createPipeline();

    filterSetup(outgoing, javaCodeGenWatch, gandivaCodeGenWatch);
  }

  // This is invoked in case of an exception to release all buffers that have been allocated
  void releaseAllBuffers() {
    for(ExpressionSplit split : this.splitExpressions) {
      split.releaseOutputBuffer();
    }
  }

  // project operator
  public void projectRecords(int recordsToConsume, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws Exception {
    try {
      for (int i = 0; i < execPipeline.size(); i++) {
        SplitStageExecutor executor = execPipeline.get(i);
        executor.evaluateProjector(recordsToConsume, javaCodeGenWatch, gandivaCodeGenWatch);
      }
    } catch (Exception e) {
      releaseAllBuffers();
      throw e;
    }
  }

  // filter data
  public int filterData(int records, Stopwatch javaCodeGenWatch, Stopwatch gandivaCodeGenWatch) throws Exception {
    try {
      for (int i = 0; i < execPipeline.size() - 1; i++) {
        SplitStageExecutor executor = execPipeline.get(i);
        executor.evaluateProjector(records, javaCodeGenWatch, gandivaCodeGenWatch);
      }

      // The last stage is the filter operation
      return execPipeline.get(execPipeline.size() - 1).evaluateFilter(records, javaCodeGenWatch, gandivaCodeGenWatch);
    } catch (Exception e) {
      releaseAllBuffers();
      throw e;
    }
  }

  @Override
  public void close() throws Exception {
    for(int i = 0; i < execPipeline.size(); i++) {
      execPipeline.get(i).close();
    }
  }

  // Generate unique name for the split
  private String getOutputNameForSplit() {
    String tempStr;

    do {
      tempStr = this.outputFieldPrefix + this.outputFieldCounter;
      this.outputFieldCounter++;
    } while (vectorContainer.getValueVectorId(SchemaPath.getSimplePath(tempStr)) != null);

    return tempStr;
  }

  private boolean gandivaSupportsReturnType(LogicalExpression expression) {
    return gandivaSplitHelper.canSplitAt(expression);
  }

  boolean canSplitAt(LogicalExpression expr, SupportedEngines.Engine engine) {
    switch (engine) {
      case GANDIVA:
        return gandivaSupportsReturnType(expr);
      case JAVA:
      default:
        // TODO: Add a generic way of indicating if Java supports split at a particular node
        return true;
    }
  }

  // Create a split at this expression
  // Adds the output field to the schema
  ExpressionSplit splitAndGenerateVectorReadExpression(CodeGenContext expr, SplitDependencyTracker
    parentTracker, SplitDependencyTracker myTracker) {
    String exprName = getOutputNameForSplit();
    SchemaPath path = SchemaPath.getSimplePath(exprName);
    FieldReference ref = new FieldReference(path);

    ExpressionSplit condSplit = myTracker.getCondSplit();
    if (condSplit != null) {
      // This is part of a nested if statement
      CodeGenContext condValueVecContext = condSplit.getReadExpressionContext();
      myTracker.addDependency(condSplit);

      // There are 2 cases: this split is part of the then expression or the else expression
      // Also, the expressions can be of the following types
      // if (c) then (if (c1) ...) else () end
      // if (c) then () else (5 + (if (c1) then (c2) else (c3) end)) end
      //
      // The split is replaced as follows:
      // then-expression: if (condValueVec) then (new split) else (null internal) end
      // else-expression: if (condValueVec) then (null internal) else (new split)
      IfExpression.Builder ifBuilder = new IfExpression.Builder().setOutputType(expr.getCompleteType());

      // Using TypedNull and not a boolean because the nested-if can be part of an expression as in:
      // if (c) then () else (5 + (if (c1) then (c2) else (c3) end)) end
      TypedNullConstant nullExpression = new TypedNullConstant(expr.getCompleteType());
      CodeGenContext nullExprContext = new CodeGenContext(nullExpression);

      if (myTracker.isPartOfThenExpr()) {
        // part of then-expr
        IfExpression.IfCondition conditions = new IfExpression.IfCondition(condValueVecContext, expr);
        ifBuilder.setIfCondition(conditions).setElse(nullExprContext);
      } else {
        // part of else-expr
        ifBuilder.setIfCondition(new IfExpression.IfCondition(condValueVecContext, nullExprContext))
          .setElse(expr);
      }

      IfExpression ifExpr = ifBuilder.build();
      CodeGenContext ifExprContext = new CodeGenContext(ifExpr);
      if (expr.isSubExpressionExecutableInEngine(this.preferredEngine) && canSplitAt(expr, this.preferredEngine)) {
        ifExprContext.addSupportedExecutionEngineForSubExpression(this.preferredEngine);
        ifExprContext.addSupportedExecutionEngineForExpression(this.preferredEngine);
      }
      expr = ifExprContext;
    }

    NamedExpression newExpr = new NamedExpression(expr, ref);
    Field outputField = expr.getCompleteType().toField(ref);
    vectorContainer.addOrGet(outputField);
    TypedFieldId fieldId = vectorContainer.getValueVectorId(ref);
    ValueVectorReadExpression read = new ValueVectorReadExpression(fieldId);
    CodeGenContext readContext = new CodeGenContext(read);
    readContext.addSupportedExecutionEngineForSubExpression(SupportedEngines.Engine.GANDIVA);
    readContext.addSupportedExecutionEngineForExpression(SupportedEngines.Engine.GANDIVA);
    // transfer the output of the new expression to the valuevectorreadexpression
    final TypedFieldId id = read.getFieldId();
    final ValueVector vvIn = vectorContainer.getValueAccessorById(id.getIntermediateClass(), id.getFieldIds()).getValueVector();

    SupportedEngines.Engine engineForSplit = expr.getExecutionEngineForSubExpression().contains(this
      .preferredEngine) ? this.preferredEngine : this.nonPreferredEngine;
    ExpressionSplit split = new ExpressionSplit(newExpr, myTracker, readContext, vvIn, false,
      engineForSplit);
    this.splitExpressions.add(split);

    parentTracker.addDependency(split);
    return split;
  }

  // Checks if the expression is a candidate for split
  // Split only functions, if expressions and boolean operators
  boolean candidateForSplit(LogicalExpression e) {

    if (e instanceof FunctionHolderExpression) {
      return true;
    }

    if (e instanceof BooleanOperator) {
      return true;
    }

    if (e instanceof IfExpression) {
      return true;
    }

    return false;
  }

  // Consider the following expression tree with 2 nodes P, and N where N is P's child
  //
  // Notation: P - PC means Node P can be evaluated in preferred codegenerator
  // P - NPC means that Node P cannot be evaluated in preferred codegenerator
  //
  // case 1) P - PC, N - PC (can split N, cannot split N): N - PC. Evaluate N in preferred code generator since N's parent
  // is going to be evaluated in preferred code generator
  // case 2) P - PC, N - NPC (can split N): N - NPC. N has to be split and can be split
  // case 3) P - PC, N - NPC (cannot split N): N - NPC. N needs to be split, but cannot. Throw an exception
  //
  // case 4) P - NPC, N - NPC (can split N, cannot split N): N - NPC
  // case 5) P - NPC, N - PC (can split N): N - PC
  // case 6) P - NPC, N - PC (cannot split N): N - NPC, if N can be executed in NPC; else throw exception
  SupportedEngines getEffectiveNodeEvaluationType(SupportedEngines parentEvalType, CodeGenContext expr)
  throws Exception {
    SupportedEngines executionEngine = expr.getExecutionEngineForExpression();

    if (parentEvalType.contains(this.preferredEngine)) {
      if (executionEngine.contains(this.preferredEngine)) {
        // both support preferred execution type
        // case 1
        return executionEngine;
      }

      if (canSplitAt(expr, this.nonPreferredEngine)) {
        // case 2
        return executionEngine;
      }

      // case 3
      throw new Exception("Node needs to be split, but cannot be split");
    }

    if (parentEvalType.contains(this.nonPreferredEngine)) {
      if (executionEngine.contains(this.preferredEngine)) {
        // needs to split, if possible
        if (canSplitAt(expr, this.preferredEngine)) {
          // case 5
          return executionEngine;
        }

        // cannot execute in preferred type
        executionEngine.remove(this.preferredEngine);
      }

      if (executionEngine.contains(this.nonPreferredEngine)) {
        // both support non preferred execution type
        // case 4
        return executionEngine;
      }

      throw new Exception("Do not know how to evaluate node");
    }

    throw new Exception("Do not know how to evaluate parent node");
  }
}
