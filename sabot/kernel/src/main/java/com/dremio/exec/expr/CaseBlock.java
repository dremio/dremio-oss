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

/**
 * Information that can be leveraged to decorate other splits (such as if-then-else, function calls,
 * nested case etc), if these splits happens inside case's <i>when</i>, <i>then</i> or <i>else</i>
 * constructs.
 *
 * <p>Case blocks could be chained for nested case statements.
 */
class CaseBlock {
  private final boolean whenSplit;
  private final ExpressionSplit prevSplit;
  private final CaseSplit currentSplit;

  CaseBlock(boolean when, ExpressionSplit prevSplit, CaseSplit currentSplit) {
    this.whenSplit = when;
    this.prevSplit = prevSplit;
    this.currentSplit = currentSplit;
  }

  /**
   * Is this a "when" split or a "then" (or "else") split?.
   *
   * @return true if it is a when split, false otherwise
   */
  public boolean isWhenSplit() {
    return whenSplit;
  }

  /**
   * Returns the dependent split for this split. If it is a "when" split it is typically the
   * previous "when" split. If it is a "then" or "else" split, this is typically the final "when"
   * split that the "then" splits are dependent on.
   *
   * @return dependent split for this, null otherwise
   */
  public ExpressionSplit getPrevSplit() {
    return prevSplit;
  }

  /**
   * Details of this split. Especially useful for nested (chained) case expressions.
   *
   * @return bookkeeping information of this split.
   */
  public CaseSplit getCurrentSplit() {
    return currentSplit;
  }

  /**
   * Wraps a split with protective if or case statements based on the level of nesting so that
   * results are always evaluated correctly.
   *
   * <p>For "when" splits, this wrapper ensures that the statement is executed if and only if the
   * previous "when" split was not evaluated. For "then" splits, this wrapper ensures that the
   * statement is executed if and only if the the evaluated "when" condition corresponds to the
   * "then" (or "else") split.
   *
   * @param expr the split expression that needs further wrapping
   * @return transformed expression with appropriate wrappers.
   */
  public CodeGenContext wrappedExpression(CodeGenContext expr) {
    return currentSplit.getModifiedExpressionForSplit(expr, prevSplit);
  }
}
