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
package com.dremio.exec.planner.cost;

import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.google.common.base.MoreObjects;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelMdSelectivity extends org.apache.calcite.rel.metadata.RelMdSelectivity {
  private static final Logger logger = LoggerFactory.getLogger(RelMdSelectivity.class);

  private static final RelMdSelectivity INSTANCE = new RelMdSelectivity(StatisticsService.NO_OP);

  public static final RelMetadataProvider SOURCE =
      ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.SELECTIVITY.method, INSTANCE);

  private final StatisticsService statisticsService;
  private boolean isNoOp;

  // from Drill
  private static final double LIKE_PREDICATE_SELECTIVITY = 0.05;

  public static final Set<SqlKind> RANGE_PREDICATE =
      EnumSet.of(
          SqlKind.LESS_THAN, SqlKind.GREATER_THAN,
          SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN_OR_EQUAL);

  public static final Set<SqlKind> SUPPORTED_REX_INPUT_PREDICATE =
      EnumSet.of(
          SqlKind.IN,
          SqlKind.LIKE,
          SqlKind.NOT_EQUALS,
          SqlKind.EQUALS,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.AND,
          SqlKind.OR,
          SqlKind.IN,
          SqlKind.NOT_IN,
          SqlKind.BETWEEN);

  public RelMdSelectivity(StatisticsService statisticsService) {
    this.statisticsService = statisticsService;
    this.isNoOp = statisticsService == StatisticsService.NO_OP;
  }

  public Double getSelectivity(SingleRel rel, RelMetadataQuery mq, RexNode predicate) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      return getSelectivity(rel.getInput(), mq, predicate);
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  public Double getSelectivity(TableScan rel, RelMetadataQuery mq, RexNode predicate) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      return getScanSelectivity(rel, mq, predicate);
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  public Double getSelectivity(TableFunctionPrel rel, RelMetadataQuery mq, RexNode predicate) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      return getScanSelectivity(rel, mq, predicate);
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  @Override
  public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      double sel = 1.0;
      if ((predicate == null) || predicate.isAlwaysTrue()) {
        return sel;
      }
      JoinRelType joinType = rel.getJoinType();
      final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
      RexNode leftPred, rightPred;
      List<RexNode> leftFilters = new ArrayList<>();
      List<RexNode> rightFilters = new ArrayList<>();
      List<RexNode> joinFilters = new ArrayList<>();
      List<RexNode> predList = RelOptUtil.conjunctions(predicate);
      RelOptUtil.classifyFilters(
          rel,
          predList,
          joinType,
          joinType == JoinRelType.INNER,
          !joinType.generatesNullsOnLeft(),
          !joinType.generatesNullsOnRight(),
          joinFilters,
          leftFilters,
          rightFilters);
      leftPred = RexUtil.composeConjunction(rexBuilder, leftFilters, true);
      rightPred = RexUtil.composeConjunction(rexBuilder, rightFilters, true);
      for (RelNode child : rel.getInputs()) {
        if (child == rel.getLeft()) {
          if (leftPred == null) {
            continue;
          }
          sel *= mq.getSelectivity(child, leftPred);
        } else {
          if (rightPred == null) {
            continue;
          }
          sel *= mq.getSelectivity(child, rightPred);
        }
      }
      // ToDo: The JoinFilters are based on the inputs but the original filter might be based on the
      // output
      sel *= guessSelectivity(RexUtil.composeConjunction(rexBuilder, joinFilters, false));
      // The remaining filters that could not be passed to the left, right or Join
      sel *= guessSelectivity(RexUtil.composeConjunction(rexBuilder, predList, false));
      return sel;
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  public Double getSelectivity(RelSubset rel, RelMetadataQuery mq, RexNode predicate) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      return mq.getSelectivity(
          MoreObjects.firstNonNull(rel.getBest(), rel.getOriginal()), predicate);
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  private Double getScanSelectivity(RelNode rel, RelMetadataQuery mq, RexNode predicate) {
    final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
    if (rel instanceof TableScan || rel instanceof TableFunctionPrel) {
      if (!DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
        return super.getSelectivity(rel, mq, predicate);
      }
      try {
        TableMetadata tableMetadata = null;
        if (rel instanceof TableFunctionPrel) {
          tableMetadata = ((TableFunctionPrel) rel).getTableMetadata();
        } else if (rel instanceof ScanRelBase) {
          tableMetadata = ((ScanRelBase) rel).getTableMetadata();
        }
        if (tableMetadata == null) {
          return super.getSelectivity(rel, mq, predicate);
        }
        List<String> fields = rel.getRowType().getFieldNames();
        return getScanSelectivityInternal(tableMetadata, predicate, fields, rexBuilder);
      } catch (Exception e) {
        logger.trace(
            String.format(
                "Exception %s occured while getting Scan Selectivity For rel %s",
                e.toString(), rel.toString()));
        return super.getSelectivity(rel, mq, predicate);
      }
    }
    return super.getSelectivity(rel, mq, predicate);
  }

  private double getScanSelectivityInternal(
      TableMetadata tableMetadata,
      RexNode predicate,
      List<String> fieldNames,
      RexBuilder rexBuilder) {
    double sel = 1.0;
    if ((predicate == null) || predicate.isAlwaysTrue()) {
      return sel;
    }
    List<RexNode> conjuncts1 = RelOptUtil.conjunctions(predicate);
    // a Set that holds range predicates that are combined based on whether they are defined on the
    // same column
    Set<RexNode> combinedRangePredicates = new HashSet<>();
    // pre-process the conjuncts such that predicates on the same column are grouped together
    List<RexNode> conjuncts2 =
        preprocessSimpleRangePredicates(
            conjuncts1, fieldNames, rexBuilder, combinedRangePredicates);
    for (RexNode pred : conjuncts2) {
      double orSel = 0;
      for (RexNode orPred : RelOptUtil.disjunctions(pred)) {
        if (isMultiColumnPredicate(orPred)) {
          orSel += guessSelectivity(orPred);
        } else if (orPred.isA(SqlKind.EQUALS)) {
          orSel += computeEqualsSelectivity(tableMetadata, orPred, fieldNames);
        } else if (orPred.isA(RANGE_PREDICATE) || combinedRangePredicates.contains(orPred)) {
          orSel += computeRangeSelectivity(tableMetadata, orPred, fieldNames);
        } else if (orPred.isA(SqlKind.NOT_EQUALS)) {
          orSel += computeNotEqualsSelectivity(tableMetadata, orPred, fieldNames);
        } else if (orPred.isA(SqlKind.LIKE)) {
          // LIKE selectivity is 5% more than a similar equality predicate, capped at CALCITE guess
          orSel +=
              Math.min(
                  computeEqualsSelectivity(tableMetadata, orPred, fieldNames)
                      + LIKE_PREDICATE_SELECTIVITY,
                  guessSelectivity(orPred));
        } else if (orPred.isA(SqlKind.NOT)) {
          RexNode childOp = ((RexCall) orPred).getOperands().get(0);
          orSel += 1.0 - getScanSelectivityInternal(tableMetadata, childOp, fieldNames, rexBuilder);
        } else if (orPred.isA(SqlKind.IS_NULL)) {
          orSel += computeNullSelectivity(tableMetadata, orPred, fieldNames);
        } else if (orPred.isA(SqlKind.IS_NOT_NULL)) {
          orSel += computeNotNullSelectivity(tableMetadata, orPred, fieldNames);
        } else {
          if (pred.equals(orPred)) {
            orSel += guessSelectivity(orPred);
          } else {
            orSel += getScanSelectivityInternal(tableMetadata, orPred, fieldNames, rexBuilder);
          }
        }
      }
      sel *= orSel;
    }

    // Cap selectivity if it exceeds 1.0
    return Math.min(sel, 1.0);
  }

  private double guessSelectivity(RexNode orPred) {
    if (logger.isTraceEnabled()) {
      logger.trace(String.format("Using guess for predicate [%s]", orPred.toString()));
    }
    // CALCITE guess
    return RelMdUtil.guessSelectivity(orPred);
  }

  // Use histogram if available for the range predicate selectivity
  private double computeRangeSelectivity(
      TableMetadata tableMetadata, RexNode orPred, List<String> fieldNames) {
    String col = getColumn(orPred, fieldNames);
    if (col != null) {
      StatisticsService.Histogram histogram = statisticsService.getHistogram(col, tableMetadata);
      if (histogram != null && histogram.isTDigestSet()) {
        Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
        Long nullCount = statisticsService.getNullCount(col, tableMetadata.getName());
        Double sel =
            (rowCount != null && nullCount != null && rowCount != 0)
                ? histogram.estimatedRangeSelectivity(orPred)
                    * (1 - (nullCount.doubleValue() / rowCount))
                : histogram.estimatedRangeSelectivity(orPred);
        if (sel != null) {
          return sel;
        }
      }
    }
    return guessSelectivity(orPred);
  }

  private double computeEqualsSelectivity(
      TableMetadata tableMetadata, RexNode orPred, List<String> fieldNames) {
    String col = getColumn(orPred, fieldNames);
    if (col != null) {
      StatisticsService.Histogram histogram = statisticsService.getHistogram(col, tableMetadata);
      Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
      if (histogram != null && histogram.isItemsSketchSet() && rowCount != null) {
        Long count = histogram.estimatedPointSelectivity(orPred);
        if (count != null && rowCount != 0) {
          return count.doubleValue() / rowCount.doubleValue();
        }
      }
      Long ndv = statisticsService.getNDV(col, tableMetadata.getName());
      if (ndv != null && ndv != 0) {
        return 1.00 / ndv;
      }
    }
    return guessSelectivity(orPred);
  }

  private double computeNotEqualsSelectivity(
      TableMetadata tableMetadata, RexNode orPred, List<String> fieldNames) {
    String col = getColumn(orPred, fieldNames);
    if (col != null) {
      StatisticsService.Histogram histogram = statisticsService.getHistogram(col, tableMetadata);
      Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
      if (histogram != null && histogram.isItemsSketchSet() && rowCount != null) {
        Long count = histogram.estimatedPointSelectivity(orPred);
        if (count != null && rowCount != 0) {
          return 1.0 - count.doubleValue() / rowCount.doubleValue();
        }
      }
      Long ndv = statisticsService.getNDV(col, tableMetadata.getName());
      if (ndv != null && ndv != 0) {
        return 1.0 - (1.00 / ndv);
      }
    }
    return guessSelectivity(orPred);
  }

  private double computeNullSelectivity(
      TableMetadata tableMetadata, RexNode orPred, List<String> fieldNames) {
    String col = getColumn(orPred, fieldNames);
    if (col != null) {
      Long nullCount = statisticsService.getNullCount(col, tableMetadata.getName());
      Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
      if (nullCount != null && rowCount != null) {
        // Cap selectivity below Calcite Guess
        return Math.min(nullCount.doubleValue() / rowCount, RelMdUtil.guessSelectivity(orPred));
      }
    }
    return guessSelectivity(orPred);
  }

  private double computeNotNullSelectivity(
      TableMetadata tableMetadata, RexNode orPred, List<String> fieldNames) {
    String col = getColumn(orPred, fieldNames);
    if (col != null) {
      Long nullCount = statisticsService.getNullCount(col, tableMetadata.getName());
      Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
      if (nullCount != null && rowCount != null) {
        // Cap selectivity below Calcite Guess
        return Math.min(
            1.0 - (nullCount.doubleValue() / rowCount), RelMdUtil.guessSelectivity(orPred));
      }
    }
    return guessSelectivity(orPred);
  }

  /**
   * Process the range predicates and combine all Simple range predicates defined on the same column
   * into a single conjunct.
   *
   * <p>For example: a > 10 AND b < 50 AND c > 20 AND a < 70 Will be combined into 3 conjuncts: (a >
   * 10 AND a < 70), (b < 50), (c > 20)
   *
   * @param conjuncts
   * @param fieldNames
   * @param rexBuilder
   * @param combinedRangePredicates
   * @return A list of predicates that includes the newly created predicate
   */
  private List<RexNode> preprocessSimpleRangePredicates(
      List<RexNode> conjuncts,
      List<String> fieldNames,
      RexBuilder rexBuilder,
      Set<RexNode> combinedRangePredicates) {
    Map<String, List<RexNode>> colToRangePredicateMap = new HashMap<>();
    List<RexNode> nonRangePredList = new ArrayList<RexNode>();
    for (RexNode pred : conjuncts) {
      if (pred.isA(RANGE_PREDICATE) && !isMultiColumnPredicate(pred)) {
        String col = getColumn(pred, fieldNames);
        if (col != null) {
          List<RexNode> predList =
              colToRangePredicateMap.computeIfAbsent(col, s -> new ArrayList<>());
          predList.add(pred);
        } else {
          nonRangePredList.add(pred);
        }
      } else {
        nonRangePredList.add(pred);
      }
    }

    List<RexNode> newPredsList = new ArrayList<>(nonRangePredList);

    // for the predicates on same column, combine them into a single conjunct
    for (Map.Entry<String, List<RexNode>> entry : colToRangePredicateMap.entrySet()) {
      List<RexNode> predList = entry.getValue();
      if (predList.size() >= 1) {
        if (predList.size() > 1) {
          RexNode newPred = RexUtil.composeConjunction(rexBuilder, predList, false);
          newPredsList.add(newPred);
          // also save this newly created predicate in a separate set for later use
          combinedRangePredicates.add(newPred);
        } else {
          newPredsList.add(predList.get(0));
        }
      }
    }

    return newPredsList;
  }

  private String getColumn(RexNode orPred, List<String> fieldNames) {
    if (orPred instanceof RexCall) {
      int colIdx = -1;
      RexInputRef op = findRexInputRef(orPred);
      if (op != null) {
        colIdx = op.getIndex();
      }
      if (colIdx != -1 && colIdx < fieldNames.size()) {
        return fieldNames.get(colIdx);
      } else {
        if (logger.isTraceEnabled()) {
          logger.trace(
              String.format(
                  "No input reference $[%s] found for predicate [%s]", colIdx, orPred.toString()));
        }
      }
    }
    return null;
  }

  private static RexInputRef findRexInputRef(final RexNode node) {
    try {
      RexVisitor<Void> visitor =
          new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {
              if (!call.isA(SUPPORTED_REX_INPUT_PREDICATE)) {
                return null;
              }
              switch (call.getKind()) {
                case AND:
                case OR:
                  for (RexNode child : call.getOperands()) {
                    child.accept(this);
                  }
                  break;
                case NOT_EQUALS:
                case EQUALS:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                case LESS_THAN_OR_EQUAL:
                case LESS_THAN:
                  // only 2 operands
                  assert (call.getOperands().size() == 2);
                  if ((call.getOperands().get(0) instanceof RexInputRef)
                      && !(call.getOperands().get(1) instanceof RexInputRef)) {
                    throw new Util.FoundOne(call.getOperands().get(0));
                  }
                  if ((call.getOperands().get(1) instanceof RexInputRef)
                      && !(call.getOperands().get(0) instanceof RexInputRef)) {
                    throw new Util.FoundOne(call.getOperands().get(1));
                  }
                  break;
                case LIKE:
                case IN:
                case BETWEEN:
                case NOT_IN:
                  // ToDO:  Histogram does not support these operations for now, so add tests when
                  // it does
                  int cnt = 0;
                  RexInputRef found = null;
                  for (RexNode child : call.getOperands()) {
                    if (child instanceof RexInputRef) {
                      found = (RexInputRef) child;
                      cnt++;
                    }
                  }
                  if (cnt == 1 && found != null) {
                    throw new Util.FoundOne(found);
                  }
                  break;
              }
              return super.visitCall(call);
            }

            @Override
            public Void visitInputRef(RexInputRef inputRef) {
              throw new Util.FoundOne(inputRef);
            }
          };
      node.accept(visitor);
      return null;
    } catch (Util.FoundOne e) {
      Util.swallow(e, null);
      return (RexInputRef) e.getNode();
    }
  }

  private boolean isMultiColumnPredicate(final RexNode node) {

    Set<Integer> rexRefs = new HashSet<Integer>();
    RexVisitor<Void> visitor =
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            rexRefs.add(inputRef.hashCode());
            return super.visitInputRef(inputRef);
          }
        };
    node.accept(visitor);
    return rexRefs.size() > 1;
  }
}
