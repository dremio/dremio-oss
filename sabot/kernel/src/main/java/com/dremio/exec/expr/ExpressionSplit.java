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

import java.util.List;
import java.util.Set;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.util.TransferPair;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.logical.data.NamedExpression;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Class captures an expression that is split
 */
public class ExpressionSplit {
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

  public SupportedEngines.Engine getExecutionEngine() {
    return executionEngine;
  }

  private final SupportedEngines.Engine executionEngine;

  ExpressionSplit(NamedExpression namedExpression, SplitDependencyTracker helper, CodeGenContext
    readExprContext, ValueVector vvOut, boolean isOriginalExpression, SupportedEngines.Engine
    executionEngine) {
    LogicalExpression expression = CodeGenerationContextRemover.removeCodeGenContext(namedExpression.getExpr());
    this.namedExpression = new NamedExpression(expression, namedExpression.getRef());
    this.executionEngine = executionEngine;
    this.isOriginalExpression = isOriginalExpression;
    this.readExpressionContext = readExprContext;
    this.vvOut = vvOut;

    this.dependsOnSplits.addAll(helper.getNamesOfDependencies());
    this.transfersIn.addAll(helper.getTransfersIn());
  }

  // All used by test code to verify correctness
  public NamedExpression getNamedExpression() { return namedExpression; }
  public String getOutputName() { return namedExpression.getRef().getAsUnescapedPath(); }
  public int getExecIteration() { return execIteration; }
  public int getTotalReadersOfOutput() { return totalReadersOfOutput; }
  public List<String> getDependencies() {
    return Lists.newArrayList(this.dependsOnSplits);
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

  // mark this split's output as being read
  private void markAsRead() {
    this.numReadersOfOutput++;
    if (this.totalReadersOfOutput == this.numReadersOfOutput) {
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
}
