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
package com.dremio.exec.planner.logical;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.arrow.util.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.WindowUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;

/**
 * <pre>
 * To rewrite percentile functions. We will consider this example query in the rule to perform the transformation:
 *
 * <b>select n_regionkey,
 * percentile_disc(0.5) within group (order by n_nationkey) as PercentileDisc_0_5,
 * percentile_cont(0.5) within group (order by n_nationkey) as PercentileCont_0_5
 * from nation
 * group by n_regionkey
 * order by n_regionkey</b>
 *
 *
 * The rewritten query:
 *
 * <b>select n_regionkey,
 * sum(PercentileDisc_0_5) as PercentileDisc_0_5,
 * sum(FloorValue) + (sum(CeilValue) - sum(FloorValue)) * mod(RowNum, FloorRowNum) as PercentileCont_0_5
 * from
 * (
 *     select n_regionkey,
 *     case when [Percentile] = 0 then (case when cum_cnt = 1 then n_nationkey else null end)
 *                              else (case when cum_cnt = ceil(0.5 * tot_cnt) then n_nationkey else null end) end as PercentileDisc_0_5,
 *
 *     (1 + (0.5 * (tot_cnt - 1))) AS RowNum,
 *     floor((1 + (0.5 * (tot_cnt - 1)))) AS FloorRowNum,
 *     case when cum_cnt = ceil(1 + (0.5 * (tot_cnt - 1))) then n_nationkey else null end as CeilValue,
 *     case when cum_cnt = floor(1 + (0.5 * (tot_cnt - 1))) then n_nationkey else null end as FloorValue
 *     from
 *     (
 *         SELECT n1.n_regionkey, n_nationkey,
 *         count(n_nationkey) over (partition by n1.n_regionkey
 *                                  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 *                                  order by n_nationkey) as cum_cnt, tot_cnt
 *         from nation n1
 *         join
 *         (
 *             select n_regionkey, count(n_nationkey) as tot_cnt from nation group by n_regionkey
 *         ) n2
 *         on n1.n_regionkey = n2.n_regionkey
 *     )
 * )
 * group by n_regionkey, RowNum, FloorRowNum
 * order by n_regionkey</b>
 *
 * </pre>
 */
public class PercentileFunctionsRewriteRule extends RelOptRule {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PercentileFunctionsRewriteRule.class);
  public static final RelOptRule INSTANCE = new PercentileFunctionsRewriteRule(DremioRelFactories.CALCITE_LOGICAL_BUILDER);

  private static final String TOTAL_COUNT = "TotalCount";
  private static final String SUM_PREFIX = "Sum";
  private static final String PERCENTILE_CONT_FIELD = "PercentileCont";
  private static final String PERCENTILE_DISC_FIELD = "PercentileDisc";
  private static final String ROW_NUMBER = "RowNum";
  private static final String FLOOR_ROW_NUMBER = "FloorRowNum";
  private static final String CEIL_VALUE = "CeilValue";
  private static final String FLOOR_VALUE = "FloorValue";

  private PercentileFunctionsRewriteRule(RelBuilderFactory factory) {
    super(operand(Aggregate.class, any()),
      factory, "AggregateFunctionsRewriteRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final List<AggregateCall> aggCalls = aggregate.getAggCallList();
    for (AggregateCall c : aggCalls) {
      final SqlKind kind = c.getAggregation().getKind();
      //Median/Percentil rewrite rule doesn't handle the filter at all currently.
      //So when it fires, it does an incorrect transformation.
      //Each rule needs to correctly transform, regardless of whatever other rules might be out there.
      //As long as the filter rewrite rule is in the same phase (or earlier) than the percentile rewrite,
      //it should be fine.
      if ((kind == SqlKind.PERCENTILE_CONT || kind == SqlKind.PERCENTILE_DISC)
          && !c.hasFilter()){
        return true;
      }
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode origInput = aggregate.getInput();
    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelBuilder relBuilder = call.builder();

    final List<AggregateCall> percentileAggs = new ArrayList<>();
    final List<AggregateCall> topAggs = new ArrayList<>();
    final Map<Integer, String> orderByCollationIndices = new HashMap<>();
    final Set<String> groupByFields = new HashSet<>(); // Define the join condition and partition by clause for "count(..) over ..."

    collectPercentileAggs(aggregate, orderByCollationIndices, percentileAggs, topAggs);
    buildJoin(relBuilder, rexBuilder, aggregate, origInput, orderByCollationIndices, groupByFields);
    buildAggregate(relBuilder, rexBuilder, aggregate, origInput, orderByCollationIndices, groupByFields, percentileAggs, topAggs);
    buildTopProject(relBuilder, rexBuilder, aggregate, groupByFields);
    call.transformTo(relBuilder.build());
  }

  private void collectPercentileAggs(Aggregate aggregate,
                                     Map<Integer, String> orderByCollationIndices,
                                     List<AggregateCall> percentileAggs,
                                     List<AggregateCall> topAggs) {
    for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
      final SqlKind kind = aggregateCall.getAggregation().getKind();
      if (kind == SqlKind.PERCENTILE_CONT || kind == SqlKind.PERCENTILE_DISC) {
        percentileAggs.add(aggregateCall);
        final List<RelFieldCollation> fieldCollations = aggregateCall.getCollation().getFieldCollations(); // Order by clause
        orderByCollationIndices.put(fieldCollations.get(0).getFieldIndex(), "");
      } else {
        topAggs.add(aggregateCall);
      }
    }
  }

  private void buildJoin(RelBuilder relBuilder,
                         RexBuilder rexBuilder,
                         Aggregate aggregate,
                         RelNode origInput,
                         Map<Integer, String> orderByCollationIndices,
                         Set<String> groupByFields) {

    // Build join's right side
    relBuilder.push(origInput);
    final List<AggregateCall> totCntAggCalls = new ArrayList<>();
    int totCntSuffix = 0;
    for (Integer index : orderByCollationIndices.keySet()) {
      String name = TOTAL_COUNT + totCntSuffix++;
      AggregateCall newCall = AggregateCall.create(
        SqlStdOperatorTable.COUNT,
        false,
        false,
        ImmutableList.of(index),
        -1,
        RelCollations.EMPTY,
        1,
        origInput,
        null,
        name);
      totCntAggCalls.add(newCall);

      // Update the name of "tot_cnt" agg function for this index. Will be needed by the case expressions at the top
      orderByCollationIndices.put(index, name);
    }

    for (int groupIndex : aggregate.getGroupSet()) {
      groupByFields.add(origInput.getRowType().getFieldList().get(groupIndex).getName());
    }

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(aggregate.getGroupSet(), null);
    relBuilder.aggregate(groupKey, totCntAggCalls);

    final RelNode right = relBuilder.build();
    final RelNode left = origInput;
    final RexNode joinCondition = getJoinCondition(left, right, groupByFields, rexBuilder);
    relBuilder.pushAll(ImmutableList.of(left, right));
    relBuilder.join(JoinRelType.LEFT, joinCondition);
  }

  private void buildAggregate(RelBuilder relBuilder,
                              RexBuilder rexBuilder,
                              Aggregate aggregate,
                              RelNode origInput,
                              Map<Integer, String> orderByCollationIndices,
                              Set<String> groupByFields,
                              List<AggregateCall> percentileAggs,
                              List<AggregateCall> topAggs) {
    final Map<Integer, Integer> percentileToCumCntMap = new HashMap<>();

    RelNode peek = relBuilder.peek();
    List<RexNode> projNodes = MoreRelOptUtil.identityProjects(peek.getRowType());

    projNodes.addAll(
      getCountWindowNodes(percentileAggs, groupByFields, origInput, peek,
        rexBuilder, projNodes.size(), percentileToCumCntMap));
    relBuilder.project(projNodes);

    peek = relBuilder.peek();
    projNodes = MoreRelOptUtil.identityProjects(peek.getRowType());
    List<String> fieldNames = new ArrayList<>(peek.getRowType().getFieldNames());
    List<List<RexNode>> caseExprs = getCaseExprs(percentileAggs, peek, rexBuilder, orderByCollationIndices, percentileToCumCntMap);
    for (int i = 0, discSuffix = 0, contSuffix = 0; i < percentileAggs.size(); i++) {
      List<RexNode> percentileProjects = caseExprs.get(i);
      final SqlKind kind = percentileAggs.get(i).getAggregation().getKind();
      switch (kind) {
        case PERCENTILE_CONT: {
          projNodes.addAll(percentileProjects);
          // Make sure the order of these fields match with the RexNodes created in getCaseExprs() function
          fieldNames.add(ROW_NUMBER + contSuffix);
          fieldNames.add(FLOOR_ROW_NUMBER + contSuffix);
          fieldNames.add(CEIL_VALUE + contSuffix);
          fieldNames.add(FLOOR_VALUE + contSuffix);
          contSuffix++;
          break;
        }
        case PERCENTILE_DISC: {
          projNodes.addAll(percentileProjects);
          fieldNames.add(PERCENTILE_DISC_FIELD + discSuffix);
          discSuffix++;
          break;
        }
        default:
          throw new RuntimeException(String.format("Unknown agg function type: %s", kind));
      }
    }
    relBuilder.project(projNodes, fieldNames);

    peek = relBuilder.peek();

    final ImmutableBitSet.Builder groupSetBuilder = ImmutableBitSet.builder().addAll(aggregate.getGroupSet());

    // Create top sums for each percentile function
    for (int i = 0, discSuffix = 0, contSuffix = 0; i < percentileAggs.size(); i++) {
      final SqlKind kind = percentileAggs.get(i).getAggregation().getKind();
      switch (kind) {
        case PERCENTILE_CONT: {
          AggregateCall sumFloorValue = createAggSum(peek, FLOOR_VALUE + contSuffix);
          AggregateCall sumCeilValue = createAggSum(peek, CEIL_VALUE + contSuffix);
          topAggs.add(sumFloorValue);
          topAggs.add(sumCeilValue);

          // Add group sets for RowNumX, FloorRowNumX
          groupSetBuilder.set(findFieldWithIndex(peek, ROW_NUMBER + contSuffix, true).left);
          groupSetBuilder.set(findFieldWithIndex(peek, FLOOR_ROW_NUMBER + contSuffix, true).left);

          contSuffix++;
          break;
        }
        case PERCENTILE_DISC: {
          topAggs.add(createAggSum(peek, PERCENTILE_DISC_FIELD + discSuffix));
          discSuffix++;
          break;
        }
        default:
          throw new RuntimeException(String.format("Unknown agg function type: %s", kind));
      }
    }

    final RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupSetBuilder.build(), null);
    relBuilder.aggregate(groupKey, topAggs);
  }

  private void buildTopProject(RelBuilder relBuilder,
                               RexBuilder rexBuilder,
                               Aggregate aggregate,
                               Set<String> groupByFields) {
    Preconditions.checkArgument(relBuilder.peek() instanceof LogicalAggregate,
      "The RelNode must be an instance of LogicalAggregate");
    LogicalAggregate peek = (LogicalAggregate) relBuilder.peek();

    final List<RexNode> projNodes = MoreRelOptUtil.identityProjects(peek.getRowType(), aggregate.getGroupSet());
    final List<String> fieldNames = new ArrayList<>(groupByFields);

    // Add top project
    int discSuffix = 0, contSuffix = 0;
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      final SqlKind kind = aggCall.getAggregation().getKind();
      OUTER: switch (kind) {
        case PERCENTILE_CONT: {
          final Pair<Integer, RelDataTypeField> sumFloorValuePair = findFieldWithIndex(peek, SUM_PREFIX + FLOOR_VALUE + contSuffix, true);
          final Pair<Integer, RelDataTypeField> sumCeilValuePair = findFieldWithIndex(peek, SUM_PREFIX + CEIL_VALUE + contSuffix, true);
          final Pair<Integer, RelDataTypeField> rowNumPair = findFieldWithIndex(peek, ROW_NUMBER + contSuffix, true);
          final Pair<Integer, RelDataTypeField> floorRowNumPair = findFieldWithIndex(peek, FLOOR_ROW_NUMBER + contSuffix, true);
          final RexNode sumFloorValue = rexBuilder.makeInputRef(peek, sumFloorValuePair.left);
          final RexNode sumCeilValue = rexBuilder.makeInputRef(peek, sumCeilValuePair.left);
          final RexNode rowNum = rexBuilder.makeInputRef(peek, rowNumPair.left);
          final RexNode floorRowNum = rexBuilder.makeInputRef(peek, floorRowNumPair.left);
          final RexNode mod = rexBuilder.makeCall(SqlStdOperatorTable.MOD, rowNum, floorRowNum);
          final RexNode multiply = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
            rexBuilder.makeCall(SqlStdOperatorTable.MINUS, sumCeilValue, sumFloorValue),
            mod);
          final RexNode sum = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, sumFloorValue, multiply);
          projNodes.add(sum);
          fieldNames.add(SUM_PREFIX + PERCENTILE_CONT_FIELD + contSuffix);
          contSuffix++;
          break;
        }
        case PERCENTILE_DISC: {
          final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(peek, SUM_PREFIX + PERCENTILE_DISC_FIELD + discSuffix, true);
          projNodes.add(rexBuilder.makeInputRef(peek, pair.left));
          fieldNames.add(SUM_PREFIX + PERCENTILE_DISC_FIELD + discSuffix);
          discSuffix++;
          break;
        }
        default:
          // Find this agg function in the newly created agg.
          for (int i = 0; i < peek.getAggCallList().size(); i++) {
            if (aggCall.equals(peek.getAggCallList().get(i))) {
              int aggCallIndex = peek.getGroupCount() + i;
              String fieldName = aggCall.getName() == null ? "EXPR$" + i : aggCall.getName();
              projNodes.add(rexBuilder.makeInputRef(peek, aggCallIndex));
              fieldNames.add(fieldName);
              break OUTER;
            }
          }
          throw new RuntimeException(String.format(
            "Unable to find agg function %s in the RelNode:\n%s",
            aggCall,
            RelOptUtil.toString(peek)));
      }
    }
    relBuilder.project(projNodes, fieldNames);

    // The percentile functions expect a double return type. See if we need to add a cast
    RelNode castProject = MoreRelOptUtil.createCastRel(relBuilder.build(), aggregate.getRowType());
    relBuilder.push(castProject);
  }

  private AggregateCall createAggSum(RelNode relNode, String fieldName) {
    final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(relNode, fieldName, true);
    return AggregateCall.create(
      SqlStdOperatorTable.SUM,
      false,
      false,
      ImmutableList.of(pair.left),
      -1,
      RelCollations.EMPTY,
      1,
      relNode,
      null,
      SUM_PREFIX + fieldName);
  }

  private RexNode getJoinCondition(RelNode left, RelNode right, Set<String> aggFields, RexBuilder rexBuilder) {
    final List<RexNode> rexNodes = new ArrayList<>();
    final RelDataType leftRowType = left.getRowType();
    final RelDataType rightRowType = right.getRowType();
    for (String field : aggFields) {
      Pair<Integer, RelDataTypeField> leftField = findFieldWithIndex(left, field, true);
      Pair<Integer, RelDataTypeField> rightField = findFieldWithIndex(right, field, true);

      final RexNode node = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
        rexBuilder.makeInputRef(leftRowType.getFieldList().get(leftField.left).getType(), leftField.left),
        rexBuilder.makeInputRef(rightRowType.getFieldList().get(rightField.left).getType(), rightField.left + leftRowType.getFieldCount()));
      rexNodes.add(node);
    }
    return RexUtil.composeConjunction(rexBuilder, rexNodes, false);
  }

  private List<RexNode> getCountWindowNodes(List<AggregateCall> percentileAggs,
                                            Set<String> groupByFields,
                                            RelNode origInput,
                                            RelNode relNode,
                                            RexBuilder rexBuilder,
                                            int index,
                                            Map<Integer, Integer> percentileToCumCntMap) {
    final Set<String> rexOverSet = new HashSet<>();
    final List<RexNode> rexOverCumCnts = new ArrayList<>();

    for (int i = 0; i < percentileAggs.size(); i++) {
      final AggregateCall aggregateCall = percentileAggs.get(i);
      final List<RexNode> partitionKeys = new ArrayList<>();
      final List<RexFieldCollation> orderKeys = new ArrayList<>();

      // Convert RelFieldCollation to RexFieldCollation
      final RelFieldCollation collation = aggregateCall.getCollation().getFieldCollations().get(0);
      final Set<SqlKind> directions = new HashSet<>();
      if (collation.direction.isDescending()) {
        directions.add(SqlKind.DESCENDING);
      }
      if (collation.nullDirection == RelFieldCollation.NullDirection.LAST) {
        directions.add(SqlKind.NULLS_LAST);
      } else if (collation.nullDirection == RelFieldCollation.NullDirection.FIRST) {
        directions.add(SqlKind.NULLS_FIRST);
      }

      final int orderByIndex = getNewFieldIndexInCurrentRel(origInput, relNode, collation.getFieldIndex());
      final RexNode orderByNode = rexBuilder.makeInputRef(relNode, orderByIndex);
      final RexFieldCollation rexCollation = new RexFieldCollation(orderByNode, directions);
      orderKeys.add(rexCollation);

      // Add partition keys
      for (String fieldName : groupByFields) {
        // Collect partition by fields
        final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(relNode, fieldName, false);
        if (pair == null) {
          continue;
        }
        partitionKeys.add(rexBuilder.makeInputRef(relNode, pair.left));
      }

      // Create RexWindow
      final RexWindow window = WindowUtil.createRexWindow(
        partitionKeys,
        orderKeys,
        RexWindowBound.create(SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO), null),
        RexWindowBound.create(SqlWindow.createCurrentRow(SqlParserPos.ZERO), null),
        true);

      final List<RexNode> operands = Collections.singletonList(orderByNode);

      final RexOver over = WindowUtil.createRexOver(
        rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
        SqlStdOperatorTable.COUNT,
        operands,
        window,
        false);

      if (!rexOverSet.contains(over.toString())) {
        rexOverSet.add(over.toString());
        rexOverCumCnts.add(over);
      }
      percentileToCumCntMap.put(i, index + rexOverCumCnts.size() - 1);
    }

    return rexOverCumCnts;
  }

  private List<List<RexNode>> getCaseExprs(List<AggregateCall> percentileAggCalls,
                                           RelNode relNode,
                                           RexBuilder rexBuilder,
                                           Map<Integer, String> orderByCollationIndices,
                                           Map<Integer, Integer> percentileToCumCntMap) {
    final List<List<RexNode>> caseExprs = new ArrayList<>();
    for (int i = 0; i < percentileAggCalls.size(); i++) {
      final AggregateCall aggregateCall = percentileAggCalls.get(i);
      final SqlKind kind = aggregateCall.getAggregation().getKind();
      switch (kind) {
        case PERCENTILE_CONT: {
          // Each PERCENTILE_CONT has a set of expressions. See the example for more details.
          caseExprs.add(getPercentileContCase(aggregateCall, relNode, rexBuilder, orderByCollationIndices, percentileToCumCntMap.get(i)));
          break;
        }
        case PERCENTILE_DISC: {
          // Each PERCENTILE_DISC function has a single case expression
          caseExprs.add(getPercentileDiscCase(aggregateCall, relNode, rexBuilder, orderByCollationIndices, percentileToCumCntMap.get(i)));
          break;
        }
        default:
          throw new RuntimeException(String.format("Unknown agg function type: %s", kind));
      }
    }
    return caseExprs;
  }

  private List<RexNode> getPercentileContCase(AggregateCall aggregateCall,
                                              RelNode relNode,
                                              RexBuilder rexBuilder,
                                              Map<Integer, String> orderByCollationIndices,
                                              Integer cumCntIndex) {
    /*
     * PERCENTILE_CONT can be calculated as:
     * FloorValue + (CeilValue – FloorValue) * (RowNum modulo FloorRowNum)
     * where RowNum = (1 + (P * (N - 1)) where P is the percentile and N is the number of rows (tot_cnt).
     *
     * Example:
     * (1 + (0.3 * (tot_cnt - 1))) AS RowNum,
     * floor(1 + (0.3 * (tot_cnt - 1))) AS FloorRowNum,
     * case when cum_cnt = ceil(1 + (0.3 * (tot_cnt - 1))) then n_nationkey else null end as CeilValue,
     * case when cum_cnt = floor(1 + (0.3 * (tot_cnt - 1))) then n_nationkey else null end as FloorValue
     */

    List<RexNode> rexNodes = new ArrayList<>();
    // Find the corresponding "tot_cnt" function for this percentile function
    final int collationIndex = aggregateCall.getCollation().getFieldCollations().get(0).getFieldIndex();
    final String totCntFunc = orderByCollationIndices.get(collationIndex);
    final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(relNode, totCntFunc, true);

    final RexNode percentile = rexBuilder.makeInputRef(relNode, aggregateCall.getArgList().get(0)); // 0.5
    final RexNode totCnt = rexBuilder.makeInputRef(relNode, pair.left); // tot_cnt
    final RexNode cumCnt = rexBuilder.makeInputRef(relNode, cumCntIndex); // cum_cnt
    final RexNode minusOne = rexBuilder.makeCall(SqlStdOperatorTable.MINUS,
      totCnt, rexBuilder.makeLiteral(BigDecimal.ONE, totCnt.getType(), false));
    final RexNode multiply = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, percentile, minusOne);
    final RexNode rowNum = rexBuilder.makeCall(
      SqlStdOperatorTable.PLUS,
      multiply,
      rexBuilder.makeLiteral(BigDecimal.ONE, multiply.getType(), false));
    final RexNode ceilRowNum = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, rowNum); // ceil(1 + (0.3 * (tot_cnt - 1)))
    RexNode floorRowNum = rexBuilder.makeCall(SqlStdOperatorTable.FLOOR, rowNum); // floor(1 + (0.3 * (tot_cnt - 1)))

    // If FloorRowNum is zero, which can happen if tot_cnt is zero in some group because
    // all the elements are null in that group, then FloorRowNum becomes zero, which will
    // cause divide by zero exception when calculating (RowNum modulo FloorRowNum).
    // Return null in that case.
    floorRowNum = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when FloorRowNum = 0
        SqlStdOperatorTable.EQUALS,
        floorRowNum,
        rexBuilder.makeLiteral(BigDecimal.ZERO, floorRowNum.getType(), false)),
      rexBuilder.constantNull(), // then null
      floorRowNum); // else FloorRowNum

    final RexNode ceilValue = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when cum_cnt = ceil(1 + (0.3 * (tot_cnt - 1)))
        SqlStdOperatorTable.EQUALS,
        cumCnt,
        ceilRowNum),
      rexBuilder.makeInputRef(relNode, collationIndex), // then n_nationkey
      rexBuilder.constantNull()); // else null

    final RexNode floorValue = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when cum_cnt = floor(1 + (0.3 * (tot_cnt - 1)))
        SqlStdOperatorTable.EQUALS,
        cumCnt,
        floorRowNum),
      rexBuilder.makeInputRef(relNode, collationIndex), // then n_nationkey
      rexBuilder.constantNull()); // else null

    // Make sure the order of these fields match with the RexNodes used in building aggregate in buildAggregate()
    rexNodes.add(rowNum);
    rexNodes.add(floorRowNum);
    rexNodes.add(ceilValue);
    rexNodes.add(floorValue);
    return rexNodes;
  }

  private List<RexNode> getPercentileDiscCase(AggregateCall aggregateCall,
                                              RelNode relNode,
                                              RexBuilder rexBuilder,
                                              Map<Integer, String> orderByCollationIndices,
                                              int cumCntIndex) {
    // Example:
    // case when Percentile = 0 then (case when cum_cnt = 1 then n_nationkey else null end)
    //                          else (case when cum_cnt = ceil(0.5 * tot_cnt) then n_nationkey else null end) end

    // Find the corresponding "tot_cnt" function for this percentile function
    final int collationIndex = aggregateCall.getCollation().getFieldCollations().get(0).getFieldIndex();
    final String totCntFunc = orderByCollationIndices.get(collationIndex);
    final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(relNode, totCntFunc, true);

    final RexNode percentile = rexBuilder.makeInputRef(relNode, aggregateCall.getArgList().get(0)); // 0.5
    final RexNode totCnt = rexBuilder.makeInputRef(relNode, pair.left); // tot_cnt
    final RexNode multiply = rexBuilder.makeCall( // 0.5 * tot_cnt
      SqlStdOperatorTable.MULTIPLY,
      totCnt,
      percentile);

    final RexNode ceil = rexBuilder.makeCall(SqlStdOperatorTable.CEIL, multiply); // ceil(0.5 * tot_cnt)
    final RexNode cumCnt = rexBuilder.makeInputRef(relNode, cumCntIndex); // cum_cnt

    final RexNode percentileCaseThen = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when cum_cnt = 1
        SqlStdOperatorTable.EQUALS,
        cumCnt,
        rexBuilder.makeLiteral(BigDecimal.ONE, cumCnt.getType(), false)),
      rexBuilder.makeInputRef(relNode, collationIndex), // then n_nationkey
      rexBuilder.constantNull()); // else null

    final RexNode percentileCaseElse = rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when cum_cnt = ceil(0.5 * tot_cnt)
        SqlStdOperatorTable.EQUALS,
        cumCnt,
        ceil),
      rexBuilder.makeInputRef(relNode, collationIndex), // then n_nationkey
      rexBuilder.constantNull() // else null
    );

    return Collections.singletonList(rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      rexBuilder.makeCall( // when Percentile = 0
        SqlStdOperatorTable.EQUALS,
        percentile,
        rexBuilder.makeZeroLiteral(percentile.getType())),
      percentileCaseThen,
      percentileCaseElse));
  }

  private int getNewFieldIndexInCurrentRel(RelNode origInput, RelNode currentRel, int index) {
    // First get the name of the field from original rel
    final String fieldName = origInput.getRowType().getFieldList().get(index).getName();
    // Now get the index of the field in the current rel
    final Pair<Integer, RelDataTypeField> pair = findFieldWithIndex(currentRel, fieldName, true);
    return pair.left;
  }

  private Pair<Integer, RelDataTypeField> findFieldWithIndex(RelNode node, String fieldName, boolean nullCheck) {
    final RelDataType rowType = node.getRowType();
    final Pair<Integer, RelDataTypeField> fieldPair = MoreRelOptUtil.findFieldWithIndex(rowType.getFieldList(), fieldName);
    if (nullCheck) {
      Preconditions.checkArgument(fieldPair != null,
        String.format("Can not find field: %s in row: %s", fieldName, rowType.toString()));
    }
    return fieldPair;
  }
}
