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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import com.dremio.common.expression.SupportedEngines;
import com.google.common.collect.Lists;

// Helper class used while splitting the expression tree
// Keeps track of state that is required as part of the split
class SplitDependencyTracker {
  // The effective evaluation type of the parent node
  private final SupportedEngines executionEngine;

  // Track the transfers into this split
  // This split depends on the output of each of these splits
  private final List<ExpressionSplit> transfersIn = new ArrayList<>();

  // The branch in nested if-expressions this expression belongs to
  private final List<IfExprBranch> ifExprBranches = new ArrayList<>();

  // case information, helps in understanding whether split is happening
  // from within a case statement and if so from which vertical (when or then)` and the
  // case condition number. The list handles multiple level of nesting of case
  private final Deque<CaseBlock> caseBlocks = new ArrayDeque<>();

  SplitDependencyTracker(SupportedEngines executionEngine, List<IfExprBranch> branchList) {
    this.executionEngine = executionEngine;
    this.ifExprBranches.addAll(branchList);
  }

  SplitDependencyTracker(SplitDependencyTracker parent) {
    this.executionEngine = parent.getExecutionEngine();
    this.ifExprBranches.addAll(parent.getIfExprBranches());
    this.caseBlocks.addAll(parent.caseBlocks);
  }

  void addIfBranch(ExpressionSplit condSplit, boolean partOfThenExpr) {
    ifExprBranches.add(new IfExprBranch(condSplit, partOfThenExpr));
  }

  void addDependency(ExpressionSplit preReq) {
    this.transfersIn.add(preReq);
    preReq.incrementReaders();
  }

  void addAllDependencies(SplitDependencyTracker helper) {
    this.transfersIn.addAll(helper.transfersIn);
  }

  void addAllDependencies(List<ExpressionSplit> dependencies) {
    this.transfersIn.addAll(dependencies);
  }

  SupportedEngines getExecutionEngine() {
    return executionEngine;
  }
  List<IfExprBranch> getIfExprBranches() { return ifExprBranches; }
  List<ExpressionSplit> getTransfersIn() {
    return transfersIn;
  }

  List<String> getNamesOfDependencies() {
    List<String> result = Lists.newArrayList();
    for(ExpressionSplit split : transfersIn) {
      result.add(split.getOutputName());
    }

    return result;
  }

  void addCaseBlock(ExpressionSplit prevSplit, CaseSplit currentSplit, boolean when) {
    caseBlocks.addLast(new CaseBlock(when, prevSplit, currentSplit));
  }

  public CodeGenContext wrapExprForCase(CodeGenContext expr, SplitDependencyTracker childTracker) {
    if (caseBlocks.isEmpty()) {
      // not from within a case block, no xformation required
      return expr;
    }
    CodeGenContext exprToReturn = expr;
    for (Iterator<CaseBlock> cbItr = caseBlocks.descendingIterator(); cbItr.hasNext();) {
      final CaseBlock nextBlock = cbItr.next();
      if (nextBlock.getPrevSplit() != null) {
        childTracker.addDependency(nextBlock.getPrevSplit());
      }
      exprToReturn = nextBlock.wrappedExpression(exprToReturn);
    }
    return exprToReturn;
  }

  /**
   * Case overhead is the number of nested case blocks + one for the extra case condition every split for a when split.
   *
   * @return overhead as an integer
   */
  public int caseSplitOverhead() {
    if (caseBlocks.isEmpty()) {
      return 0;
    }
    int overhead = caseBlocks.size() - 1;
    if (caseBlocks.getLast().isWhenSplit()) {
      overhead += 1;
    }
    return overhead;
  }

  public void removeLastCaseBlock() {
    caseBlocks.removeLast();
  }
}
