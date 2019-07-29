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
  private final List<ExpressionSplit> transfersIn = Lists.newArrayList();

  // The branch in nested if-expressions this expression belongs to
  private final List<IfExprBranch> ifExprBranches = Lists.newArrayList();

  SplitDependencyTracker(SupportedEngines executionEngine, List<IfExprBranch> branchList) {
    this.executionEngine = executionEngine;
    this.ifExprBranches.addAll(branchList);
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
}
