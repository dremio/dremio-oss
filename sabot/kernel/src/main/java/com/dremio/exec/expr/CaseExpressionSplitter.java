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

import com.dremio.common.expression.CaseExpression;
import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.LogicalExpression;
import com.dremio.common.expression.TypedNullConstant;
import java.util.ArrayList;
import java.util.List;

/** Splits a case expression depending on the output of {@code CaseExpressionAnalyzer}. */
public class CaseExpressionSplitter {
  private final CaseSplits caseSplits;
  private final CodeGenContext originalExprWithContext;
  private final CaseExpression originalExpr;
  private final int origSz;
  private final ExpressionSplitter splitter;
  private final SplitDependencyTracker parentTracker;
  private final PreferenceBasedSplitter ifSplitter;

  CaseExpressionSplitter(
      CodeGenContext originalExpression,
      SplitDependencyTracker parentTracker,
      ExpressionSplitter splitter,
      CaseSplits split,
      PreferenceBasedSplitter ifSplitter) {
    this.caseSplits = split;
    this.originalExprWithContext = originalExpression;
    this.originalExpr = (CaseExpression) originalExpression.getChild();
    // total conditions + else expression
    this.origSz = this.originalExpr.caseConditions.size() + 1;
    this.parentTracker = parentTracker;
    this.splitter = splitter;
    this.ifSplitter = ifSplitter;
  }

  public CodeGenContext splitAndGetFinalSplit() throws Exception {
    final CaseSplit firstWhenSplit = caseSplits.getNextWhenSplit();
    assert firstWhenSplit != null;
    if (firstWhenSplit.hasZeroSplits()) {
      // short circuit if entire case statement can be executed in preferred.
      return firstWhenSplit.getSplitWithContext(originalExprWithContext.getChild());
    }
    ExpressionSplit finalWhenSplit = doAllWhenSplits(firstWhenSplit);
    return doAllThenSplits(finalWhenSplit);
  }

  /**
   * Slice the "when" vertical split further based on information given by the analyzer.
   *
   * <p>"when" splits cannot be combined as order matters for "when" split to adhere to the case
   * expression semantics.
   *
   * @param firstWhenSplit the first "when" split marker
   * @return the actual final "when" split whose value vector will contain information about which
   *     "when" condition got evaluated. This information can then be used to wrap the "then"
   *     splits.
   * @throws Exception when the splitter fails for some reason
   */
  private ExpressionSplit doAllWhenSplits(CaseSplit firstWhenSplit) throws Exception {
    CaseSplit currentWhenSplit = firstWhenSplit;
    ExpressionSplit finalVwSplit = null;
    do {
      switch (currentWhenSplit.getSplitType()) {
        case MIXED:
          finalVwSplit = doMixedWhenSplit(currentWhenSplit, finalVwSplit);
          break;
        case PREFERRED:
        case NON_PREFERRED:
          finalVwSplit = doNormalWhenSplit(currentWhenSplit, finalVwSplit);
          break;
      }
      currentWhenSplit = caseSplits.getNextWhenSplit();
    } while (currentWhenSplit != null);
    return finalVwSplit;
  }

  /**
   * Slices the "then" (or "else") vertical splits based on the execution engine.
   *
   * <p>Note that then splits need not have order as they execute based on the result of "when"
   * splits. This allows the "then" splits to be compressed into max two splits (one for preferred
   * and one for non-preferred).
   *
   * @param vwSplit the final "when" split which provides a value vector containing the final result
   *     of the "when" vertical split.
   * @return Final "then" split
   * @throws Exception when the splitter runs into issues.
   */
  private CodeGenContext doAllThenSplits(ExpressionSplit vwSplit) throws Exception {
    final List<CaseSplit> mixedThenSplits = caseSplits.getAllMixedThenSplits();
    final List<CaseSplit> nonPreferredThenSplits = caseSplits.getAllNonPreferredThenSplits();
    final List<CaseSplit> preferredThenSplits = caseSplits.getAllPreferredThenSplits();
    MixedThenTracker mixedInfo = processMixedThenSplits(mixedThenSplits, vwSplit);
    switch (mixedInfo.getEffectiveSplitType(nonPreferredThenSplits, preferredThenSplits)) {
      case MIXED:
        final ExpressionSplit prevThenSplit =
            doIntermediateThenSplit(nonPreferredThenSplits, vwSplit, mixedInfo);
        return doFinalThenSplit(preferredThenSplits, vwSplit, prevThenSplit, mixedInfo, true);

      case NON_PREFERRED:
        return doFinalThenSplit(nonPreferredThenSplits, vwSplit, null, mixedInfo, false);

      case PREFERRED:
      default:
        return doFinalThenSplit(preferredThenSplits, vwSplit, null, mixedInfo, true);
    }
  }

  private ExpressionSplit doNormalWhenSplit(CaseSplit whenSplit, ExpressionSplit prevSplit) {
    List<CaseExpression.CaseConditionNode> newConditions = new ArrayList<>();
    if (prevSplit != null) {
      // add protective condition, in case the prev split was successful
      final LogicalExpression firstWhenExpr = whenSplit.getWhenSkipperExpression(prevSplit);
      final LogicalExpression firstThenExpr = prevSplit.getReadExpressionContext();
      newConditions.add(new CaseExpression.CaseConditionNode(firstWhenExpr, firstThenExpr));
    }
    int currIdx = whenSplit.getConditionStartIndex();
    for (; currIdx < whenSplit.getConditionEndIndex() && currIdx < origSz - 1; currIdx++) {
      final LogicalExpression whenExpr = originalExpr.caseConditions.get(currIdx).whenExpr;
      final LogicalExpression thenExpr = whenSplit.getIntExpression(currIdx);
      newConditions.add(new CaseExpression.CaseConditionNode(whenExpr, thenExpr));
    }
    final LogicalExpression elseExpr = whenSplit.getElseExpressionWhenSplit();
    final CodeGenContext splitContext =
        whenSplit.getSplitWithContext(
            CaseExpression.newBuilder()
                .setCaseConditions(newConditions)
                .setElseExpr(elseExpr)
                .setOutputType(CompleteType.INT)
                .build());
    return doCaseSplit(splitContext, whenSplit, prevSplit, null);
  }

  private ExpressionSplit doMixedWhenSplit(CaseSplit whenSplit, ExpressionSplit prevWhenSplit)
      throws Exception {
    final CodeGenContext whenExpr =
        (CodeGenContext)
            originalExpr.caseConditions.get(whenSplit.getConditionStartIndex()).whenExpr;
    final SplitDependencyTracker myTracker = new SplitDependencyTracker(parentTracker);
    myTracker.addCaseBlock(prevWhenSplit, whenSplit, true);
    final int currIdx = whenSplit.getConditionStartIndex();
    final CodeGenContext convertedContext = CaseFunctions.convertCaseToIf(whenExpr);
    final CodeGenContext splitWhenExpr = convertedContext.accept(ifSplitter, myTracker);
    myTracker.removeLastCaseBlock();
    List<CaseExpression.CaseConditionNode> newConditions = new ArrayList<>();
    if (prevWhenSplit != null) {
      // add protective condition, in case the prev split was successful
      final LogicalExpression firstWhenExpr = whenSplit.getWhenSkipperExpression(prevWhenSplit);
      final LogicalExpression firstThenExpr = prevWhenSplit.getReadExpressionContext();
      newConditions.add(new CaseExpression.CaseConditionNode(firstWhenExpr, firstThenExpr));
    }
    newConditions.add(
        new CaseExpression.CaseConditionNode(splitWhenExpr, whenSplit.getIntExpression(currIdx)));
    final LogicalExpression elseExpr = whenSplit.getElseExpressionWhenSplit();
    final CodeGenContext splitContext =
        whenSplit.getSplitWithContext(
            CaseExpression.newBuilder()
                .setCaseConditions(newConditions)
                .setElseExpr(elseExpr)
                .setOutputType(CompleteType.INT)
                .build());
    whenSplit.addEngineToContextMixed(splitWhenExpr, splitContext);
    return doCaseSplit(splitContext, whenSplit, prevWhenSplit, myTracker);
  }

  private ExpressionSplit doIntermediateThenSplit(
      List<CaseSplit> thenSplits, ExpressionSplit vwSplit, MixedThenTracker mixedInfo) {
    final SplitDependencyTracker myTracker = new SplitDependencyTracker(parentTracker);
    final CodeGenContext splitContext =
        createNormalThenContext(thenSplits, vwSplit, null, true, mixedInfo, false);
    myTracker.addDependency(vwSplit);
    mixedInfo.addMixedDependencies(false, myTracker);
    return splitter.splitAndGenerateVectorReadExpression(
        splitContext, new NullDependencyTracker(parentTracker), myTracker);
  }

  private CodeGenContext doFinalThenSplit(
      List<CaseSplit> thenSplits,
      ExpressionSplit vwSplit,
      ExpressionSplit prevThenSplit,
      MixedThenTracker mixedInfo,
      boolean preferred) {
    final CodeGenContext splitContext =
        createNormalThenContext(thenSplits, vwSplit, prevThenSplit, false, mixedInfo, preferred);
    if (prevThenSplit != null) {
      parentTracker.addDependency(prevThenSplit);
    }
    mixedInfo.addMixedDependencies(preferred, parentTracker);
    return splitContext;
  }

  CodeGenContext createNormalThenContext(
      List<CaseSplit> thenSplits,
      ExpressionSplit vwSplit,
      ExpressionSplit prevThenSplit,
      boolean intermediate,
      MixedThenTracker mixedInfo,
      boolean preferred) {
    List<CaseExpression.CaseConditionNode> newConditions = new ArrayList<>();
    mixedInfo.fillConditions(newConditions, preferred);
    CodeGenContext elseExpr =
        (prevThenSplit != null)
            ? prevThenSplit.getReadExpressionContext()
            : CodeGenContext.buildWithAllEngines(
                new TypedNullConstant(originalExpr.getCompleteType()));
    if (!thenSplits.isEmpty()) {
      final CodeGenContext leftExpr = vwSplit.getReadExpressionContext();
      CaseSplit finalSplit = thenSplits.get(0);
      for (CaseSplit thenSplit : thenSplits) {
        int currIdx = thenSplit.getConditionStartIndex();
        for (; currIdx < thenSplit.getConditionEndIndex() && currIdx < origSz - 1; currIdx++) {
          final LogicalExpression whenExpr = thenSplit.getEqualExpression(leftExpr, currIdx);
          final LogicalExpression thenExpr = originalExpr.caseConditions.get(currIdx).thenExpr;
          newConditions.add(new CaseExpression.CaseConditionNode(whenExpr, thenExpr));
        }
        if (finalSplit.getConditionEndIndex() < thenSplit.getConditionEndIndex()) {
          finalSplit = thenSplit;
        }
      }
      final int finalIdx = finalSplit.getConditionEndIndex() - 1;
      if (finalIdx == origSz - 1) {
        if (prevThenSplit != null || intermediate) {
          final LogicalExpression whenExpr = finalSplit.getEqualExpression(leftExpr, finalIdx);
          final LogicalExpression thenExpr = originalExpr.elseExpr;
          newConditions.add(new CaseExpression.CaseConditionNode(whenExpr, thenExpr));
        } else {
          elseExpr = (CodeGenContext) originalExpr.elseExpr;
        }
      }
    }
    final CodeGenContext context =
        CodeGenContext.buildWithNoDefaultSupport(
            CaseExpression.newBuilder()
                .setCaseConditions(newConditions)
                .setElseExpr(elseExpr)
                .setOutputType(originalExpr.getCompleteType())
                .build());
    context.addSupportedExecutionEngineForSubExpression(
        (preferred) ? ifSplitter.preferredEngine : ifSplitter.nonPreferredEngine);
    context.addSupportedExecutionEngineForExpression(
        (preferred) ? ifSplitter.preferredEngine : ifSplitter.nonPreferredEngine);
    return context;
  }

  private MixedThenTracker processMixedThenSplits(
      List<CaseSplit> mixedThenSplits, ExpressionSplit vwSplit) throws Exception {
    final MixedThenTracker mixedThenTracker = new MixedThenTracker();
    if (mixedThenSplits.isEmpty()) {
      return mixedThenTracker;
    }
    final CodeGenContext leftExpr = vwSplit.getReadExpressionContext();
    for (CaseSplit thenSplit : mixedThenSplits) {
      int currIdx = thenSplit.getConditionStartIndex();
      final SplitDependencyTracker tracker = new SplitDependencyTracker(parentTracker);
      final LogicalExpression whenExpr = thenSplit.getEqualExpression(leftExpr, currIdx);
      tracker.addCaseBlock(vwSplit, thenSplit, false);
      CodeGenContext splitThenExpr =
          (currIdx >= origSz - 1)
              ? ((CodeGenContext) originalExpr.elseExpr).accept(ifSplitter, tracker)
              : ((CodeGenContext) originalExpr.caseConditions.get(currIdx).thenExpr)
                  .accept(ifSplitter, tracker);
      tracker.removeLastCaseBlock();
      mixedThenTracker.addCondition(whenExpr, splitThenExpr, tracker);
    }
    return mixedThenTracker;
  }

  /**
   * Do a case condition split.
   *
   * <p>For a case condition split, an intermediate "when" or "then" split will have only dependency
   * to previous case split and so it should not be tied to the parent tracker. Only the final
   * "when" or "then" split will be tied to the parent tracker.
   *
   * @param splitWithContext the case split expression
   * @param currentSplit the case split information used to create current case split
   * @param prevSplit the actual previous case expression split
   * @param mixedTracker split dependency tracker for mixed "when", "then" or "else" expressions
   *     that belongs to this case split, null if no mixed expression in this case split.
   * @return a case split as an expression split
   */
  private ExpressionSplit doCaseSplit(
      CodeGenContext splitWithContext,
      CaseSplit currentSplit,
      ExpressionSplit prevSplit,
      SplitDependencyTracker mixedTracker) {
    final SplitDependencyTracker myTracker = new SplitDependencyTracker(parentTracker);
    if (mixedTracker != null) {
      // add all dependencies of mixed tracker to my tracker (w.o) increasing reader count as final
      // expr
      // is mered with the case.
      myTracker.addAllDependencies(mixedTracker);
    }
    if (prevSplit != null) {
      myTracker.addDependency(prevSplit);
    }
    if (caseSplits.isLastSplit(currentSplit)) {
      return splitter.splitAndGenerateVectorReadExpression(
          splitWithContext, parentTracker, myTracker);
    } else {
      return splitter.splitAndGenerateVectorReadExpression(
          splitWithContext, new NullDependencyTracker(parentTracker), myTracker);
    }
  }

  /**
   * This avoids double counting of readers in the split as the intermediate splits have to be
   * already tracked outside.
   */
  private static class NullDependencyTracker extends SplitDependencyTracker {
    NullDependencyTracker(SplitDependencyTracker parent) {
      super(parent);
    }

    @Override
    void addDependency(ExpressionSplit preReq) {}
  }

  private class MixedThenTracker {
    private final List<CaseExpression.CaseConditionNode> preferredConditions;
    private final List<CaseExpression.CaseConditionNode> nonPreferredConditions;
    private final List<ExpressionSplit> preferredSplitDependencies;
    private final List<ExpressionSplit> nonPreferredSplitDependencies;

    private MixedThenTracker() {
      preferredConditions = new ArrayList<>();
      nonPreferredConditions = new ArrayList<>();
      preferredSplitDependencies = new ArrayList<>();
      nonPreferredSplitDependencies = new ArrayList<>();
    }

    private void addCondition(
        LogicalExpression whenExpr, CodeGenContext splitThenExpr, SplitDependencyTracker tracker) {
      if (splitThenExpr.isExpressionExecutableInEngine(ifSplitter.preferredEngine)) {
        preferredConditions.add(new CaseExpression.CaseConditionNode(whenExpr, splitThenExpr));
        preferredSplitDependencies.addAll(tracker.getTransfersIn());
      } else {
        nonPreferredConditions.add(new CaseExpression.CaseConditionNode(whenExpr, splitThenExpr));
        nonPreferredSplitDependencies.addAll(tracker.getTransfersIn());
      }
    }

    public CaseSplit.CaseSplitType getEffectiveSplitType(
        List<CaseSplit> nonPreferredThenSplits, List<CaseSplit> preferredThenSplits) {
      final boolean preferred = !preferredThenSplits.isEmpty() || !preferredConditions.isEmpty();
      final boolean nonPreferred =
          !nonPreferredThenSplits.isEmpty() || !nonPreferredConditions.isEmpty();
      return (preferred && nonPreferred
          ? CaseSplit.CaseSplitType.MIXED
          : (preferred)
              ? CaseSplit.CaseSplitType.PREFERRED
              : CaseSplit.CaseSplitType.NON_PREFERRED);
    }

    public void fillConditions(
        List<CaseExpression.CaseConditionNode> newConditions, boolean preferred) {
      if (preferred) {
        newConditions.addAll(preferredConditions);
      } else {
        newConditions.addAll(nonPreferredConditions);
      }
    }

    public void addMixedDependencies(boolean preferred, SplitDependencyTracker tracker) {
      if (preferred) {
        tracker.addAllDependencies(preferredSplitDependencies);
      } else {
        tracker.addAllDependencies(nonPreferredSplitDependencies);
      }
    }
  }
}
