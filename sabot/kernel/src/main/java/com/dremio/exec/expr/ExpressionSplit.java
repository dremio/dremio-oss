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

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.logical.data.NamedExpression;
import com.dremio.exec.record.TypedFieldId;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.llvm.ExpressionWorkEstimator;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

/** Class captures an expression that is split */
public class ExpressionSplit extends CachableExpressionSplit implements Closeable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(ExpressionSplit.class);
  // This vector captures the output when the split is executed
  // The data from this value vector has to be transferred to vvOut and to all
  // readers of the output of this split
  private ValueVector outputOfSplit = null;

  // The split. This could be an intermediate root-node or the expression root
  private NamedExpression namedExpression;

  // The value vector representing the ValueVectorRead representing this split
  private final ValueVector vvOut;
  // Transfers output of this split. This will only contain vvOut (if non-null)
  private final List<TransferPair> transferPairList = Lists.newArrayList();

  // The number of readers that have read the intermediate output from this split
  // When this matches totalReadersOfOutput, the ValueVector holding the output (vvOut)
  // can be released
  private int numReadersOfOutput = 0;

  // All pre-req splits whose output is read by this split
  // On evaluting this split, all pre-req splits are informed that their output has
  // been read
  private List<ExpressionSplit> transfersIn = Lists.newArrayList();

  // The iteration in which this split is executed. For testing purposes only
  private int execIteration;

  private Set<String> dependsOnSplits = new HashSet<>();

  public TypedFieldId getTypedFieldId() {
    return typedFieldId;
  }

  private final TypedFieldId typedFieldId;

  // ValueVectorReadExpression representing the output of this split
  private final CodeGenContext readExpressionContext;

  ExpressionSplit(
      CachableExpressionSplit cachableExpressionSplit,
      ValueVector vvOut,
      NamedExpression namedExpression,
      TypedFieldId fieldId,
      CodeGenContext readExprContext) {
    super(cachableExpressionSplit);
    LogicalExpression expression =
        CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr());
    this.vvOut = vvOut;
    this.namedExpression = new NamedExpression(expression, namedExpression.getRef());
    this.typedFieldId = fieldId;
    this.readExpressionContext = readExprContext;
  }

  ExpressionSplit(
      NamedExpression namedExpression,
      SplitDependencyTracker helper,
      TypedFieldId fieldId,
      CodeGenContext readExprContext,
      ValueVector vvOut,
      boolean isOriginalExpression,
      SupportedEngines.Engine executionEngine,
      int numExtraIfExprs,
      OperatorContext operatorContext) {
    super(
        namedExpression,
        isOriginalExpression,
        operatorContext,
        executionEngine,
        helper,
        numExtraIfExprs,
        fieldId);
    LogicalExpression expression =
        CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr());
    this.vvOut = vvOut;
    this.transfersIn.addAll(helper.getTransfersIn());
    this.namedExpression = new NamedExpression(expression, namedExpression.getRef());
    this.typedFieldId = fieldId;
    this.dependsOnSplits.addAll(helper.getNamesOfDependencies());
    this.readExpressionContext = readExprContext;
  }

  public Set<String> getDependsOnSplits() {
    return dependsOnSplits;
  }

  @Override
  public String getOutputName() {
    return namedExpression.getRef().getAsUnescapedPath();
  }

  public void setDependsOnSplits(Set<String> dependsOnSplits) {
    this.dependsOnSplits = dependsOnSplits;
  }

  @Override
  public String toString() {
    StringBuilder dependsOn = new StringBuilder();
    for (String str : getDependsOnSplits()) {
      dependsOn.append(str).append(" ");
    }

    String fieldIdStr = "null";
    if (typedFieldId != null) {
      fieldIdStr = typedFieldId.toString();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("name: ")
        .append(namedExpression.getRef())
        .append(", fieldId: ")
        .append(fieldIdStr)
        .append(", readers: ")
        .append(getTotalReadersOfOutput())
        .append(", dependencies: ")
        .append(dependsOn)
        .append(", expr: ")
        .append(namedExpression.getExpr());

    return sb.toString();
  }

  // All used by test code to verify correctness
  public int getExecIteration() {
    return execIteration;
  }

  public List<String> getDependencies() {
    return Lists.newArrayList(getDependsOnSplits());
  }

  void setOutputOfSplit(ValueVector valueVector) {
    this.outputOfSplit = valueVector;
    if (vvOut != null) {
      transferPairList.add(outputOfSplit.makeTransferPair(vvOut));
    }
  }

  void transferOut() {
    for (TransferPair transferPair : transferPairList) {
      transferPair.transfer();
    }
  }

  // Mark output of pre-req splits as read
  void markOutputAsRead() {
    for (ExpressionSplit split : transfersIn) {
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
    LogicalExpression expr = namedExpression.getExpr();
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
  void markAsRead() {
    this.numReadersOfOutput++;
    if (getTotalReadersOfOutput() == this.numReadersOfOutput) {
      if (logger.isTraceEnabled()) {
        logger.trace(
            "Releasing output buffer for {}, fieldid {}",
            getOutputName(),
            typedFieldId != null ? typedFieldId.toString() : "null");
      }
      releaseOutputBuffer();
    }
  }

  void setExecIteration(int iteration) {
    this.execIteration = iteration;
  }

  public List<ExpressionSplit> getTransfersIn() {
    return transfersIn;
  }

  public void setTransfersIn(List<ExpressionSplit> transfersIn) {
    this.transfersIn = transfersIn;
  }

  public CodeGenContext getReadExpressionContext() {
    return readExpressionContext;
  }

  public NamedExpression getNamedExpression() {
    return namedExpression;
  }

  public void setNamedExpression(NamedExpression namedExpression) {
    this.namedExpression = namedExpression;
  }

  @Override
  public void close() throws IOException {
    transfersIn.clear();
  }
}
