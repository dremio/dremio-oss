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
package com.dremio.exec.expr;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.record.TypedFieldId;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Class captures an expression that is split
 */
public class ExpressionSplit implements Closeable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ExpressionSplit.class);

  // The split. This could be an intermediate root-node or the expression root
  private NamedExpression namedExpression;

  // Set of splits on which this split depends
  private final Set<String> dependsOnSplits = Sets.newHashSet();

  // true, if the output of this split, is the output of the original expression
  private final boolean isOriginalExpression;

  // This vector captures the output when the split is executed
  // The data from this value vector has to be transferred to vvOut and to all
  // readers of the output of this split
  private ValueVector outputOfSplit = null;

  // ValueVectorReadExpression representing the output of this split
  private final CodeGenContext readExpressionContext;
  // The value vector representing the ValueVectorRead representing this split
  private final ValueVector vvOut;
  // Transfers output of this split. This will only contain vvOut (if non-null)
  private final List<TransferPair> transferPairList = Lists.newArrayList();

  // number of readers of the output from this split
  private int totalReadersOfOutput = 0;

  // The number of readers that have read the intermediate output from this split
  // When this matches totalReadersOfOutput, the ValueVector holding the output (vvOut)
  // can be released
  private int numReadersOfOutput = 0;

  // All pre-req splits whose output is read by this split
  // On evaluting this split, all pre-req splits are informed that their output has
  // been read
  private final List<ExpressionSplit> transfersIn = Lists.newArrayList();

  // The iteration in which this split is executed. For testing purposes only
  private int execIteration;

  // The number of extra if expressions created due to this split
  private int numExtraIfExprs;

  final private TypedFieldId typedFieldId;
  private String toStr = null;

  public SupportedEngines.Engine getExecutionEngine() {
    return executionEngine;
  }

  private final SupportedEngines.Engine executionEngine;

  ExpressionSplit(NamedExpression namedExpression, SplitDependencyTracker helper, TypedFieldId fieldId,
                  CodeGenContext readExprContext, ValueVector vvOut, boolean isOriginalExpression, SupportedEngines.Engine
    executionEngine, int numExtraIfExprs) {
    LogicalExpression expression = CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr());
    this.namedExpression = new NamedExpression(expression, namedExpression.getRef());
    this.executionEngine = executionEngine;
    this.typedFieldId = fieldId;
    this.isOriginalExpression = isOriginalExpression;
    this.readExpressionContext = readExprContext;
    this.vvOut = vvOut;

    this.dependsOnSplits.addAll(helper.getNamesOfDependencies());
    this.transfersIn.addAll(helper.getTransfersIn());
    this.numExtraIfExprs = numExtraIfExprs;
  }

  public String toString() {
    if (toStr == null) {
      String dependsOn = "";
      for (String str : dependsOnSplits) {
        dependsOn += str + " ";
      }

      String fieldIdStr = "null";
      if (typedFieldId != null) {
        fieldIdStr = typedFieldId.toString();
      }

      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("name: " + namedExpression.getRef().toString());
      stringBuilder.append(", fieldId: " + fieldIdStr);
      stringBuilder.append(", readers: " + totalReadersOfOutput);
      stringBuilder.append(", dependencies: " + dependsOn);
      stringBuilder.append(", expr: " + namedExpression.getExpr().toString());

      toStr = stringBuilder.toString();
    }

    return toStr;
  }

  // All used by test code to verify correctness
  public NamedExpression getNamedExpression() { return namedExpression; }
  public String getOutputName() { return namedExpression.getRef().getAsUnescapedPath(); }
  public int getExecIteration() { return execIteration; }
  public int getTotalReadersOfOutput() { return totalReadersOfOutput; }
  public List<String> getDependencies() {
    return Lists.newArrayList(this.dependsOnSplits);
  }
  int getOverheadDueToExtraIfs() {
    return numExtraIfExprs;
  }

  CodeGenContext getReadExpressionContext() { return readExpressionContext; }
  void setOutputOfSplit(ValueVector valueVector) {
    this.outputOfSplit = valueVector;
    if (vvOut != null) {
      transferPairList.add(outputOfSplit.makeTransferPair(vvOut));
    }
  }

  void transferOut() {
    for(TransferPair transferPair : transferPairList) {
      transferPair.transfer();
    }
  }

  // Mark output of pre-req splits as read
  void markOutputAsRead() {
    for(ExpressionSplit split : transfersIn) {
      split.markAsRead();
    }
  }

  // release the output buffer
  void releaseOutputBuffer() {
    if (vvOut != null) {
      // all transfers done
      this.vvOut.clear();
    }
    // reset for the next batch
    this.numReadersOfOutput = 0;
  }

  // returns a double that quantifies amount of work done by this split
  double getWork() {
    LogicalExpression expr = getNamedExpression().getExpr();
    ExpressionWorkEstimator workEstimator = new ExpressionWorkEstimator();
    double result = expr.accept(workEstimator, null);

    // Some if expressions can lead to additional work. For e.g. when the then and else
    // expressions of an if-expression are evaluated in different engines, this leads to
    // an overhead of evaluating one extra if-expression
    //
    // Such work is treated as an overhead and subtracted from the actual work done
    return result;
  }

  // mark this split's output as being read
  private void markAsRead() {
    this.numReadersOfOutput++;
    if (this.totalReadersOfOutput == this.numReadersOfOutput) {
      logger.trace("Releasing output buffer for {}, fieldid {}", getOutputName(), typedFieldId != null ? typedFieldId.toString() : "null");
      releaseOutputBuffer();
    }
  }

  // increment the readers of this split
  void incrementReaders() {
    this.totalReadersOfOutput++;
  }

  boolean isOriginalExpression() { return isOriginalExpression; }

  void setExecIteration(int iteration) {
    this.execIteration = iteration;
  }

  @Override
  public void close() throws IOException {
    transfersIn.clear();
  }

  // Estimates the cost of evaluating an expression
  // Can enhance this later to provide additional weight for specific Gandiva/Java functions
  // TODO: Improve the definition of work
  // For now, functions and if-expr contribute 1 to the work
  class ExpressionWorkEstimator extends AbstractExprVisitor<Double, Void, RuntimeException> {
    @Override
    public Double visitFunctionHolderExpression(FunctionHolderExpression holder, Void value) throws RuntimeException {
      double result = 1.0;

      for(LogicalExpression arg : holder.args) {
        result += arg.accept(this, null);
      }

      return result;
    }

    @Override
    public Double visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
      double result = 1.0;

      result += ifExpr.ifCondition.condition.accept(this, null);
      result += ifExpr.ifCondition.expression.accept(this, null);
      result += ifExpr.elseExpression.accept(this, null);

      return result;
    }

    @Override
    public Double visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
      double result = 1.0;

      for(LogicalExpression arg : op.args) {
        result += arg.accept(this, null);
      }

      return result;
    }

    @Override
    public Double visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
      return 0.0;
    }
  }
}
