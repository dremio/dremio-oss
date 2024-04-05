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

import com.dremio.common.expression.BooleanOperator;
import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.FunctionHolderExpression;
import com.dremio.common.expression.IfExpression;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.SupportedEngines;
import com.dremio.common.expression.visitors.AbstractExprVisitor;
import com.dremio.exec.compile.sig.ConstantExpressionIdentifier;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Case expression splitter analyzer. Does in-depth analysis and splits a case expression, such that
 * each sub-expression can be evaluated either in Gandiva or Java. This analyzer is effectively a
 * helper class for {@link CaseExpressionSplitter} and it does a recursive analysis of whether the
 * expression needs a split and if so provides the splitter information on how to do the splits
 * through the {@link CaseSplits} object.
 *
 * <p>The case expression splitting is done in a different way to IfExpr Splitting, where initially
 * the case expression is split into two vertical splits (when_split and then_split) and based on
 * further analysis split further into multiple horizontal splits in both the "when" and "then"
 * vertical splits.
 *
 * <p>
 */
public class CaseExpressionAnalyzer {
  private static final CaseSplits WRAPPED_ZERO_SPLIT_PREFERRED =
      new ZeroCaseSplits(CaseSplit.getZeroSplit(CaseSplit.CaseSplitType.PREFERRED));
  private static final CaseSplits WRAPPED_ZERO_SPLIT_NON_PREFERRED =
      new ZeroCaseSplits(CaseSplit.getZeroSplit(CaseSplit.CaseSplitType.NON_PREFERRED));
  private static final CaseSplits WRAPPED_ZERO_SPLIT_PREFERRED_FLIPPED =
      new ZeroCaseSplits(CaseSplit.getZeroSplitFlipped(CaseSplit.CaseSplitType.PREFERRED));
  private static final CaseSplits WRAPPED_ZERO_SPLIT_NON_PREFERRED_FLIPPED =
      new ZeroCaseSplits(CaseSplit.getZeroSplitFlipped(CaseSplit.CaseSplitType.NON_PREFERRED));

  private enum CodegenState {
    FULL(CaseSplit.CaseSplitType.PREFERRED, false, true), // Fully Executable in Preferred
    MIXED(CaseSplit.CaseSplitType.MIXED, false, false), // sub expressions is mixed
    NONE(
        CaseSplit.CaseSplitType.NON_PREFERRED,
        false,
        true), // Entire condition executable in non-preferred only
    ANY(CaseSplit.CaseSplitType.PREFERRED, true, true),
    NA(CaseSplit.CaseSplitType.PREFERRED, true, true); // Not applicable

    private final CaseSplit.CaseSplitType splitType;
    private final boolean allCompatible;
    private final boolean sameCompatible;

    CodegenState(CaseSplit.CaseSplitType splitType, boolean allCompatible, boolean sameCompatible) {
      this.splitType = splitType;
      this.allCompatible = allCompatible;
      this.sameCompatible = sameCompatible;
    }

    CaseSplit.CaseSplitType toSplitType() {
      return splitType;
    }

    boolean isNotCompatible(CodegenState other) {
      return !allCompatible && (!sameCompatible || other != this);
    }
  }

  private final SupportedEngines.Engine preferredEngine;
  private final SupportedEngines.Engine nonPreferredEngine;

  CaseExpressionAnalyzer(
      SupportedEngines.Engine preferredEngine, SupportedEngines.Engine nonPreferredEngine) {
    this.preferredEngine = preferredEngine;
    this.nonPreferredEngine = nonPreferredEngine;
  }

  /**
   * Does a top down analysis of a case expression and provides information about how best to split
   * it in the {@link CaseSplits} object.
   *
   * @param context case expression with code gen context.
   * @return Information on how to split the case expression end to end.
   */
  CaseSplits analyzeCaseExpression(CodeGenContext context) {
    final CaseExpression caseExpression = (CaseExpression) context.getChild();
    if (context.isSubExpressionExecutableInEngine(preferredEngine)) {
      // no split required as all sub expressions are executable in preferred engine
      return preferred();
    }
    final int totalConditions = caseExpression.caseConditions.size();
    final CodegenSummary[] codegenSummaries = new CodegenSummary[totalConditions + 1];
    final boolean needsSplit = summarizeCodegen(caseExpression, codegenSummaries);
    if (needsSplit) {
      rewriteAnyStates(codegenSummaries);
      return buildCaseSplit(codegenSummaries);
    }
    return (codegenSummaries[0].whenState == CodegenState.FULL) ? preferred() : nonPreferred();
  }

  private CaseSplits preferred() {
    return (preferredEngine.equals(SupportedEngines.Engine.GANDIVA))
        ? WRAPPED_ZERO_SPLIT_PREFERRED
        : WRAPPED_ZERO_SPLIT_PREFERRED_FLIPPED;
  }

  private CaseSplits nonPreferred() {
    return (nonPreferredEngine.equals(SupportedEngines.Engine.JAVA))
        ? WRAPPED_ZERO_SPLIT_NON_PREFERRED
        : WRAPPED_ZERO_SPLIT_NON_PREFERRED_FLIPPED;
  }

  private MultiCaseSplits buildCaseSplit(CodegenSummary[] codegenSummaries) {
    // decide on split by checking compatibility
    final MultiCaseSplits caseSplits =
        new MultiCaseSplits(
            codegenSummaries.length, this.preferredEngine != SupportedEngines.Engine.GANDIVA);
    final int lastIdx = codegenSummaries.length - 1;
    CodegenSummary previousSummary = null;
    int conditionIdx = 0;
    int lastWhenSplitIdx = 0;
    int lastThenSplitIdx = 0;
    for (final CodegenSummary summary : codegenSummaries) {
      if (previousSummary != null) {
        if (summary.isHorizontalWhenSplitHere(previousSummary)) {
          caseSplits.addWhenSplit(
              conditionIdx, codegenSummaries[conditionIdx - 1].whenState.toSplitType());
          lastWhenSplitIdx = conditionIdx;
        }
        if (summary.isHorizontalThenOrElseSplitHere(previousSummary)) {
          caseSplits.addThenSplit(
              conditionIdx, codegenSummaries[conditionIdx - 1].thenOrElseState.toSplitType());
          lastThenSplitIdx = conditionIdx;
        }
      }
      previousSummary = summary;
      conditionIdx++;
    }
    if (lastWhenSplitIdx < lastIdx) {
      // lastIdx is inclusive, whereas created split has end index that is exclusive
      caseSplits.addWhenSplit(lastIdx + 1, codegenSummaries[lastIdx - 1].whenState.toSplitType());
    }
    if (lastThenSplitIdx <= lastIdx) {
      caseSplits.addThenSplit(lastIdx + 1, codegenSummaries[lastIdx].thenOrElseState.toSplitType());
    }
    return caseSplits;
  }

  /**
   * Rewrite "ANY" codegen states. "ANY" codegen state is given to "when" or "then" expressions that
   * are fully constant and is executable in both engines. "ANY" state is rewritten the appropriate
   * state to get the most efficient splits.
   *
   * @param codegenSummaries current codegen summary
   */
  private void rewriteAnyStates(CodegenSummary[] codegenSummaries) {
    final Deque<Range> whenList = new ArrayDeque<>();
    final Deque<Range> thenList = new ArrayDeque<>();

    for (int i = 0; i < codegenSummaries.length; i++) {
      CodegenSummary cs = codegenSummaries[i];
      if (cs.whenState == CodegenState.ANY) {
        if (whenList.isEmpty() || whenList.getLast().end < i - 1) {
          whenList.addLast(new Range(i, true));
        } else {
          whenList.getLast().setEnd(i);
        }
      }
      if (cs.thenOrElseState == CodegenState.ANY) {
        if (thenList.isEmpty() || thenList.getLast().end < i - 1) {
          thenList.addLast(new Range(i, false));
        } else {
          thenList.getLast().setEnd(i);
        }
      }
    }
    whenList.forEach((r) -> r.deriveAndRewriteState(codegenSummaries));
    thenList.forEach((r) -> r.deriveAndRewriteState(codegenSummaries));
  }

  private static final class Range {
    private final int start;
    private final boolean when;
    private int end;

    private Range(int start, boolean when) {
      this.start = start;
      this.end = start;
      this.when = when;
    }

    public void setEnd(int end) {
      this.end = end;
    }

    public void deriveAndRewriteState(CodegenSummary[] codegenSummaries) {
      CodegenState prevState = CodegenState.NA;
      CodegenState nextState = CodegenState.NA;
      final int lastIdx = (when) ? codegenSummaries.length - 2 : codegenSummaries.length - 1;
      if (start > 0) {
        prevState =
            (when)
                ? codegenSummaries[start - 1].whenState
                : codegenSummaries[start - 1].thenOrElseState;
      }
      if (end < lastIdx) {
        nextState =
            (when)
                ? codegenSummaries[end + 1].whenState
                : codegenSummaries[end + 1].thenOrElseState;
      }
      CodegenState newState;
      if (prevState != CodegenState.NA && nextState != CodegenState.NA) {
        // we are fully bounded
        if (prevState == CodegenState.FULL || nextState == CodegenState.FULL) {
          newState = CodegenState.FULL;
        } else if (prevState == CodegenState.NONE || nextState == CodegenState.NONE) {
          newState = CodegenState.NONE;
        } else {
          newState = CodegenState.FULL;
        }
      } else {
        // on the top or bottom edge
        newState =
            (prevState == CodegenState.NA && nextState == CodegenState.NA)
                ? CodegenState.FULL
                : (prevState == CodegenState.NA) ? nextState : prevState;
        if (newState == CodegenState.MIXED) {
          newState = CodegenState.FULL;
        }
      }
      for (int i = start; i <= end; i++) {
        if (when) {
          codegenSummaries[i] = new CodegenSummary(newState, codegenSummaries[i].thenOrElseState);
        } else {
          codegenSummaries[i] = new CodegenSummary(codegenSummaries[i].whenState, newState);
        }
      }
    }
  }

  /**
   * Summarize codegen of every expression/sub expression of the case statement.
   *
   * @param caseExpression expression to inspect and summarize
   * @param conditionSummary summarized data
   */
  private boolean summarizeCodegen(
      CaseExpression caseExpression, CodegenSummary[] conditionSummary) {
    int i = 0;
    boolean needsSplit = false;
    for (final CaseExpression.CaseConditionNode node : caseExpression.caseConditions) {
      CodeGenContext whenContext = (CodeGenContext) node.whenExpr;
      CodeGenContext thenContext = (CodeGenContext) node.thenExpr;
      final CodegenSummary thisSummary =
          new CodegenSummary(computeCodegenState(whenContext), computeCodegenState(thenContext));
      if (!needsSplit) {
        needsSplit = thisSummary.needsSplit();
        if (i > 0 && !needsSplit) {
          needsSplit = thisSummary.needsSplit(conditionSummary[i - 1]);
        }
      }
      conditionSummary[i++] = thisSummary;
    }
    CodeGenContext elseContext = (CodeGenContext) caseExpression.elseExpr;
    conditionSummary[i] = new CodegenSummary(computeCodegenState(elseContext));
    if (!needsSplit) {
      needsSplit = conditionSummary[i].needsSplit();
      if (!needsSplit) {
        needsSplit = conditionSummary[i].needsSplit(conditionSummary[i - 1]);
      }
    }
    return needsSplit;
  }

  private CodegenState computeCodegenState(CodeGenContext exprContext) {
    SupportedEngines engines =
        findLeastCommon(
            exprContext.getExecutionEngineForExpression(),
            exprContext.getExecutionEngineForSubExpression());
    int engineCount = engines.size();
    if (engineCount == 0) {
      return CodegenState.MIXED;
    }
    if (engineCount == 1) {
      return engines.contains(preferredEngine)
          ? CodegenState.FULL
          : mixedIfAnyPreferred(exprContext);
    }
    // if engine count is 2, but if entire expression is constant, let us put ANY as don't care
    return (ConstantExpressionIdentifier.isExpressionConstant(exprContext.getChild()))
        ? CodegenState.ANY
        : CodegenState.FULL;
  }

  private CodegenState mixedIfAnyPreferred(CodeGenContext exprContext) {
    final Boolean ret = exprContext.getChild().accept(new PreferredEngineChecker(), null);
    return (ret != null && ret) ? CodegenState.MIXED : CodegenState.NONE;
  }

  private SupportedEngines findLeastCommon(
      SupportedEngines executionEngineForExpression,
      SupportedEngines executionEngineForSubExpression) {
    final SupportedEngines ret = new SupportedEngines();
    if (executionEngineForExpression.contains(preferredEngine)
        && executionEngineForSubExpression.contains(preferredEngine)) {
      ret.add(preferredEngine);
    }
    final int exprEngineCount = executionEngineForExpression.size();
    final int subExprEngineCount = executionEngineForSubExpression.size();
    if ((exprEngineCount == 2 && subExprEngineCount == 2)
        || (exprEngineCount == 1
            && subExprEngineCount == 1
            && executionEngineForExpression.contains(nonPreferredEngine)
            && executionEngineForSubExpression.contains(nonPreferredEngine))) {
      ret.add(nonPreferredEngine);
    }
    return ret;
  }

  private static final class CodegenSummary {
    private final CodegenState whenState;
    private final CodegenState thenOrElseState;

    private CodegenSummary(CodegenState thenOrElseState) {
      this.whenState = CodegenState.NA;
      this.thenOrElseState = thenOrElseState;
    }

    private CodegenSummary(CodegenState whenState, CodegenState thenOrElseState) {
      this.whenState = whenState;
      this.thenOrElseState = thenOrElseState;
    }

    boolean isHorizontalWhenSplitHere(CodegenSummary prev) {
      return whenState.isNotCompatible(prev.whenState);
    }

    boolean isHorizontalThenOrElseSplitHere(CodegenSummary prev) {
      return thenOrElseState.isNotCompatible(prev.thenOrElseState);
    }

    boolean needsSplit() {
      return whenState.isNotCompatible(thenOrElseState);
    }

    boolean needsSplit(CodegenSummary prevSummary) {
      return thenOrElseState.isNotCompatible(prevSummary.thenOrElseState)
          || whenState.isNotCompatible(prevSummary.whenState)
          || whenState.isNotCompatible(prevSummary.thenOrElseState)
          || thenOrElseState.isNotCompatible(prevSummary.whenState);
    }
  }

  /**
   * Implementation of Case Splits when the analyzer decides on zero splits as the expression can be
   * executed in preferred engine in its entirety OR the expression has no sub expression candidate
   * that can execute in preferred.
   */
  private static final class ZeroCaseSplits implements CaseSplits {
    private final CaseSplit singleSplit;

    private ZeroCaseSplits(CaseSplit singleSplit) {
      this.singleSplit = singleSplit;
    }

    @Override
    public CaseSplit getNextWhenSplit() {
      return singleSplit;
    }

    @Override
    public boolean isLastSplit(CaseSplit split) {
      return true;
    }
  }

  /** Implementation of {@code CaseSplits}, when there is some splitting of case required. */
  public static final class MultiCaseSplits implements CaseSplits {
    private final Deque<CaseSplit> whenSplits;
    private final Deque<CaseSplit> thenSplits;
    private final int numThenOrElse;
    private final boolean flipped;

    private int prevWhenSplitEnd;
    private int prevThenSplitEnd;

    MultiCaseSplits(int numThenOrElse, boolean flipped) {
      this.numThenOrElse = numThenOrElse;
      this.whenSplits = new ArrayDeque<>();
      this.thenSplits = new ArrayDeque<>();
      this.prevWhenSplitEnd = 0;
      this.prevThenSplitEnd = 0;
      this.flipped = flipped;
    }

    void addWhenSplit(int conditionIndex, CaseSplit.CaseSplitType splitType) {
      assert conditionIndex >= prevWhenSplitEnd;
      final CaseSplit caseSplit =
          new CaseSplit(prevWhenSplitEnd, conditionIndex, numThenOrElse, true, splitType, flipped);
      prevWhenSplitEnd = conditionIndex;
      whenSplits.offerLast(caseSplit);
    }

    void addThenSplit(int conditionIndex, CaseSplit.CaseSplitType splitType) {
      assert conditionIndex >= prevThenSplitEnd;
      final CaseSplit caseSplit =
          new CaseSplit(prevThenSplitEnd, conditionIndex, numThenOrElse, false, splitType, flipped);
      prevThenSplitEnd = conditionIndex;
      thenSplits.offerLast(caseSplit);
    }

    @Override
    public CaseSplit getNextWhenSplit() {
      return whenSplits.poll();
    }

    @Override
    public boolean isLastSplit(CaseSplit split) {
      return split.isPartOfWhen() ? this.whenSplits.isEmpty() : this.thenSplits.isEmpty();
    }

    @Override
    public List<CaseSplit> getAllMixedThenSplits() {
      return thenSplits.stream()
          .filter((s) -> s.getSplitType() == CaseSplit.CaseSplitType.MIXED)
          .collect(Collectors.toList());
    }

    @Override
    public List<CaseSplit> getAllNonPreferredThenSplits() {
      return thenSplits.stream()
          .filter((s) -> s.getSplitType() == CaseSplit.CaseSplitType.NON_PREFERRED)
          .collect(Collectors.toList());
    }

    @Override
    public List<CaseSplit> getAllPreferredThenSplits() {
      return thenSplits.stream()
          .filter((s) -> s.getSplitType() == CaseSplit.CaseSplitType.PREFERRED)
          .collect(Collectors.toList());
    }
  }

  /**
   * Visits expression tree recursively to identify whether there is an expression executable in
   * preferred below in the sub tree, even though the current expression and sub expression is
   * executable only in non-preferred engine.
   */
  class PreferredEngineChecker extends AbstractExprVisitor<Boolean, Void, RuntimeException> {
    @Override
    public Boolean visitCaseExpression(CaseExpression caseExpression, Void value)
        throws RuntimeException {
      return checkChildren(caseExpression);
    }

    @Override
    public Boolean visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
      return checkChildren(op);
    }

    @Override
    public Boolean visitFunctionHolderExpression(FunctionHolderExpression holder, Void value)
        throws RuntimeException {
      return checkChildren(holder);
    }

    @Override
    public Boolean visitIfExpression(IfExpression ifExpr, Void value) throws RuntimeException {
      return checkChildren(ifExpr);
    }

    @Override
    public Boolean visitUnknown(LogicalExpression expr, Void value) throws RuntimeException {
      return false;
    }

    private boolean checkChildren(LogicalExpression childExpr) {
      if (ConstantExpressionIdentifier.isExpressionConstant(childExpr)) {
        if (childExpr instanceof CodeGenContext
            && ((CodeGenContext) childExpr).getExecutionEngineForExpression().size() >= 2
            && ((CodeGenContext) childExpr).getExecutionEngineForSubExpression().size() >= 2) {
          // no need to go deep into a constant expression that is supported by both engines
          return false;
        }
      }
      boolean ret = false;
      for (LogicalExpression e : childExpr) {
        if (ret) {
          break;
        }
        if (e instanceof CodeGenContext) {
          final CodeGenContext context = (CodeGenContext) e;
          ret =
              (context.getExecutionEngineForExpression().contains(preferredEngine)
                  || context.getExecutionEngineForSubExpression().contains(preferredEngine));
          if (ret) {
            ret = canCheckForPreferred(context.getChild());
          }
          if (!ret) {
            ret = context.getChild().accept(this, null);
          }
        }
      }
      return ret;
    }

    private boolean canCheckForPreferred(LogicalExpression expr) {
      if (expr instanceof FunctionHolderExpression) {
        return !CaseFunctions.isCommonToAllEngines(((FunctionHolderExpression) expr).getName());
      }
      return expr instanceof IfExpression || expr instanceof BooleanOperator;
    }
  }
}
