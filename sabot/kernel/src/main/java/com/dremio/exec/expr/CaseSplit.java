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

import java.util.Arrays;
import java.util.List;

import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.TypedNullConstant;
import com.dremio.common.expression.ValueExpressions;

/**
 * Individual split of a case expression.
 *
 * <p>
 * startIndex is inclusive and endIndex is exclusive. An endIndex of case expressions condition list size
 * indicates the else expression. From a vertical slicing perspective, both "then" and "else" expressions are on the
 * same vertical side, while "when" expressions are on the other side of the vertical.
 * </p>
 */
public class CaseSplit {
  private static final CaseSplit PREFERRED_ZERO_SPLIT = new CaseSplit(CaseSplit.CaseSplitType.PREFERRED, false);
  private static final CaseSplit NON_PREFERRED_ZERO_SPLIT = new CaseSplit(CaseSplit.CaseSplitType.NON_PREFERRED, false);
  private static final CaseSplit PREFERRED_ZERO_SPLIT_FLIPPED = new CaseSplit(CaseSplit.CaseSplitType.PREFERRED, true);
  private static final CaseSplit NON_PREFERRED_ZERO_SPLIT_FLIPPED =
    new CaseSplit(CaseSplit.CaseSplitType.NON_PREFERRED, true);

  public enum CaseSplitType {
    PREFERRED,
    NON_PREFERRED,
    MIXED
  }

  private final int conditionStartIndex;
  private final int conditionEndIndex;
  private final boolean partOfWhen;
  private final boolean zeroSplit;
  private final CaseSplitType splitType;
  private final int origSz;
  private final boolean flipped;

  public CaseSplit(int start, int end, int origSz, boolean when, CaseSplitType splitType, boolean flipped) {
    this.conditionStartIndex = start;
    this.conditionEndIndex = end;
    this.partOfWhen = when;
    this.splitType = splitType;
    this.origSz = origSz;
    this.zeroSplit = false;
    this.flipped = flipped;
  }

  private CaseSplit(CaseSplitType splitType, boolean flipped) {
    this.conditionStartIndex = -1;
    this.conditionEndIndex = -1;
    this.origSz = -1;
    this.partOfWhen = false;
    this.zeroSplit = true;
    this.splitType = splitType;
    this.flipped = flipped;
  }

  public static CaseSplit getZeroSplit(CaseSplitType splitType) {
    return splitType == CaseSplitType.PREFERRED ? PREFERRED_ZERO_SPLIT : NON_PREFERRED_ZERO_SPLIT;
  }

  public static CaseSplit getZeroSplitFlipped(CaseSplitType splitType) {
    return splitType == CaseSplitType.PREFERRED ? PREFERRED_ZERO_SPLIT_FLIPPED : NON_PREFERRED_ZERO_SPLIT_FLIPPED;
  }

  public int getConditionStartIndex() {
    return this.conditionStartIndex;
  }

  public int getConditionEndIndex() {
    return this.conditionEndIndex;
  }

  public boolean isPartOfWhen() {
    return this.partOfWhen;
  }

  public boolean hasZeroSplits() {
    return zeroSplit;
  }

  public CaseSplitType getSplitType() {
    return this.splitType;
  }

  /**
   * Get a constant int expression wrapped with a codegen context annotated with engine type based on this split type.
   * @param value the int literal
   * @return a codegen context with a constant int logical expression
   */
  public CodeGenContext getIntExpression(int value) {
    assert !zeroSplit;
    CodeGenContext context = CodeGenContext.buildWithNoDefaultSupport(ValueExpressions.getInt(value));
    addEngineToContext(context);
    return context;
  }

  /**
   * Get an appropriate "else" expression based on the end index of this split.
   * <p>
   * If the end index does not correspond to the size of the original expression, then this "when" split is a
   * continuation and so the else expression should return a -1, otherwise it should return a positive integer
   * that corresponds to the index of the actual "else" expression.
   * </p>
   * @return a codegen context with a constant int expression appropriate for this "when" split
   */
  public CodeGenContext getElseExpressionWhenSplit() {
    assert !zeroSplit;
    final int value = (conditionEndIndex >= origSz) ? origSz - 1 : -1;
    CodeGenContext context = CodeGenContext.buildWithNoDefaultSupport(ValueExpressions.getInt(value));
    addEngineToContext(context);
    return context;
  }

  /**
   * Returns function expression "equal(left, right)". Used in then splits.
   * @param leftExpr lhs
   * @param right rhs
   * @return a codegen context with equal expression
   */
  public CodeGenContext getEqualExpression(CodeGenContext leftExpr, int right) {
    assert !zeroSplit;
    final LogicalExpression rightExpr = getIntExpression(right);
    final List<LogicalExpression> args = Arrays.asList(leftExpr, rightExpr);
    final CodeGenContext context = CodeGenContext.buildWithNoDefaultSupport(
      CaseFunctions.getInstance().getEqFnExpr(args));
    addEngineToContext(context);
    return context;
  }

  /**
   * Expression that skips this "when" split, in case the previous when split was evaluated.
   * @param prevSplit Previous expression split from which we can read the result of previous "when" split
   * @return a codegen context that wraps a function expression which acts as a condition to skip this "when" split
   */
  public CodeGenContext getWhenSkipperExpression(ExpressionSplit prevSplit) {
    assert !zeroSplit;
    final LogicalExpression leftExpr = prevSplit.getReadExpressionContext();
    final LogicalExpression rightExpr = getIntExpression(-1);
    final List<LogicalExpression> args = Arrays.asList(leftExpr, rightExpr);
    final CodeGenContext context = CodeGenContext.buildWithNoDefaultSupport(
      CaseFunctions.getInstance().getGtFnExpr(args));
    addEngineToContext(context);
    return context;
  }

  public CodeGenContext getSplitWithContext(LogicalExpression expr) {
    final CodeGenContext context = CodeGenContext.buildWithNoDefaultSupport(expr);
    addEngineToContext(context);
    return context;
  }

  public CodeGenContext getModifiedExpressionForSplit(CodeGenContext expr, ExpressionSplit prevSplit) {
    if (prevSplit == null) {
      // no wrapper required as there is no prev when split
      return expr;
    }
    final CodeGenContext nullExprContext = new CodeGenContext(new TypedNullConstant(expr.getCompleteType()));
    if (partOfWhen) {
      // this is a split happening inside a "when" expression of case, so this split should be changed
      // to the form (if lessThan(prevSplit, 0) then expr else null (of expr return type)
      final LogicalExpression fnExpr = getWhenExpressionWrapperFunction(expr, prevSplit);
      return CodeGenContext.buildWithCurrentCodegen(expr, new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(fnExpr, expr))
        .setElse(nullExprContext)
        .setOutputType(expr.getCompleteType())
        .build());
    } else {
      // this is a split happening inside a "then" expression of case, so this split should be changed
      // to the form (if equal(prevSplit, <conditionIdx>) then expr else null (of expr return type)
      final LogicalExpression fnExpr = getThenExpressionWrapperFunction(expr, prevSplit);
      return CodeGenContext.buildWithCurrentCodegen(expr, new IfExpression.Builder()
        .setIfCondition(new IfExpression.IfCondition(fnExpr, expr))
        .setElse(nullExprContext)
        .setOutputType(expr.getCompleteType())
        .build());
    }
  }

  public void addEngineToContextMixed(CodeGenContext splitWhenExpr, CodeGenContext splitContext) {
    if (this.splitType.equals(CaseSplitType.MIXED)) {
      final SupportedEngines.Engine p = (flipped) ? SupportedEngines.Engine.JAVA : SupportedEngines.Engine.GANDIVA;
      final SupportedEngines.Engine np = (flipped) ? SupportedEngines.Engine.GANDIVA : SupportedEngines.Engine.JAVA;
      if (splitWhenExpr.isExpressionExecutableInEngine(p)) {
        splitContext.addSupportedExecutionEngineForSubExpression(p);
        splitContext.addSupportedExecutionEngineForExpression(p);
      } else if (splitWhenExpr.isExpressionExecutableInEngine(np)) {
        splitContext.addSupportedExecutionEngineForSubExpression(np);
        splitContext.addSupportedExecutionEngineForExpression(np);
      }
    }
  }

  private CodeGenContext getWhenExpressionWrapperFunction(CodeGenContext whenExpr, ExpressionSplit prevSplit) {
    assert !zeroSplit;
    final LogicalExpression leftExpr = prevSplit.getReadExpressionContext();
    final LogicalExpression rightExpr = getIntExpression(0);
    final List<LogicalExpression> args = Arrays.asList(leftExpr, rightExpr);
    return CodeGenContext.buildWithCurrentCodegen(whenExpr,
      CaseFunctions.getInstance().getLtFnExpr(args));
  }

  private CodeGenContext getThenExpressionWrapperFunction(CodeGenContext whenExpr, ExpressionSplit vwSplit) {
    assert !zeroSplit;
    final LogicalExpression leftExpr = vwSplit.getReadExpressionContext();
    final LogicalExpression rightExpr = getIntExpression(this.conditionStartIndex);
    final List<LogicalExpression> args = Arrays.asList(leftExpr, rightExpr);
    return CodeGenContext.buildWithCurrentCodegen(whenExpr,
      CaseFunctions.getInstance().getEqFnExpr(args));
  }

  private void addEngineToContext(CodeGenContext context) {
    switch (this.splitType) {
      case PREFERRED:
        SupportedEngines.Engine preferred = (flipped) ? SupportedEngines.Engine.JAVA : SupportedEngines.Engine.GANDIVA;
        context.addSupportedExecutionEngineForSubExpression(preferred);
        context.addSupportedExecutionEngineForExpression(preferred);
        break;

      case NON_PREFERRED:
        SupportedEngines.Engine np = (flipped) ? SupportedEngines.Engine.GANDIVA : SupportedEngines.Engine.JAVA;
        context.addSupportedExecutionEngineForSubExpression(np);
        context.addSupportedExecutionEngineForExpression(np);
        break;

      case MIXED:
      default:
        break;
    }
  }
}
