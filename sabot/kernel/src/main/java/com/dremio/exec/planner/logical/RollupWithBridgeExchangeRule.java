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
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder.GroupKey;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;


public class RollupWithBridgeExchangeRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new RollupWithBridgeExchangeRule();
  private final RelBuilderFactory factory;
  public RollupWithBridgeExchangeRule() {
    super(RelOptHelper.some(AggregateRel.class, RelOptHelper.any(RelNode.class)), "GroupingSetsRule");
    this.factory = DremioRelFactories.LOGICAL_BUILDER;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    if (agg.getGroupSets() == null || agg.getGroupSets().size() <= 1) {
    return false;
    }
    if (!isRollup(agg.getGroupSets())) {
      return false;
    }
    return true;
  }
  @Override
  public void onMatch(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    RelNode input = agg.getInput();
    RelBuilder relBuilder = (RelBuilder) factory.create(agg.getCluster(), null);
    RelMetadataQuery mq = agg.getCluster().getMetadataQuery();

    relBuilder.push(input);

    RelDataType bridgeRowType;
    Iterator<ImmutableBitSet> groupSetIterator = agg.getGroupSets().stream().iterator();
    long hash = MoreRelOptUtil.longHashCode(agg);
    String id = Long.toHexString(hash);
    int groupCount = agg.getGroupCount();
    {
      ImmutableBitSet groupSet = groupSetIterator.next();
      aggregate(groupSet, agg, relBuilder, false, groupCount);
      groupCount = groupSet.cardinality();
      BridgeExchangeRel bridge = new BridgeExchangeRel(agg.getCluster(), agg.getTraitSet(), relBuilder.build(), id);
      bridgeRowType = bridge.getRowType();
      relBuilder.push(bridge);
      project(agg, agg.getGroupSet(), agg.getCluster().getRexBuilder(), relBuilder);
    }
    while (groupSetIterator.hasNext()) {
      BridgeReaderRel bridgeReader = new BridgeReaderRel(agg.getCluster(), agg.getTraitSet(), bridgeRowType, relBuilder.peek().estimateRowCount(mq), id);
      relBuilder.push(bridgeReader);
      ImmutableBitSet groupSet = groupSetIterator.next();
      aggregate(groupSet, agg, relBuilder, true, groupCount);
      groupCount = groupSet.cardinality();
      if (groupSetIterator.hasNext()) {
        id = Long.toHexString(System.nanoTime());
        BridgeExchangeRel bridge = new BridgeExchangeRel(agg.getCluster(), agg.getTraitSet(), relBuilder.build(), id);
        bridgeRowType = bridge.getRowType();
        relBuilder.push(bridge);
      }
      project(agg, groupSet, agg.getCluster().getRexBuilder(), relBuilder);
      relBuilder.union(true);
    }
    RelNode result = relBuilder.build();
    call.transformTo(result);
  }

  public static boolean isRollup(ImmutableList<ImmutableBitSet> groupSets) {
    if (groupSets.size() <= 1) {
      return false;
    }
    for (int i  = 1; i < groupSets.size(); i++) {
      ImmutableBitSet s1 = groupSets.get(i - 1);
      ImmutableBitSet s2 = groupSets.get(i);
      if (!s1.contains(s2)) {
        return false;
      }
    }
    return true;
  }
  public static boolean isGrouping(AggregateCall call) {
    return call.getAggregation().getKind() == SqlKind.GROUPING_ID || call.getAggregation().getKind() == SqlKind.GROUPING;
  }
  private static List<AggregateCall> transformAggCalls(List<AggregateCall> calls, boolean transform, int offset) {
    List<AggregateCall> groupingRemoved = calls.stream().filter(c -> !isGrouping(c)).collect(Collectors.toList());
    return Ord.zip(groupingRemoved).stream()
      .map(call -> {
        AggregateCall c = call.e;
        SqlAggFunction func = c.getAggregation();
        if (transform && c.getAggregation().getKind() == SqlKind.COUNT) {
          func = SqlStdOperatorTable.SUM0;
        }
        List<Integer> args;
        if (transform) {
          args = Collections.singletonList(offset + call.i);
        } else {
          args = c.getArgList();
        }
        return AggregateCall.create(
          func,
          c.isDistinct(),
          c.isApproximate(),
          args,
          c.filterArg,
          c.collation,
          c.getType(),
          c.getName());
      })
      .collect(Collectors.toList());
  }

  private static void aggregate(ImmutableBitSet groupSet, Aggregate agg, RelBuilder relBuilder, boolean transform, int groupCount) {
    GroupKey groupKey = relBuilder.groupKey(groupSet);
    relBuilder.aggregate(groupKey, transformAggCalls(agg.getAggCallList(), transform, groupCount));
  }
  private static void project(Aggregate agg, ImmutableBitSet groupSet, RexBuilder rexBuilder, RelBuilder relBuilder) {
    List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < agg.getGroupSet().cardinality(); i++) {
      if (groupSet.get(i)) {
        RelDataType type = relBuilder.peek().getRowType().getFieldList().get(i).getType();
        projects.add(new RexInputRef(i, type));
      } else {
        projects.add(rexBuilder.makeNullLiteral(agg.getRowType().getFieldList().get(i).getType()));
      }
    }
    final int aggGroupSetSize = agg.getGroupSet().cardinality();
    int inputRef = groupSet.cardinality();
    for (int i = 0; i < agg.getAggCallList().size(); i++) {
      if (isGrouping(agg.getAggCallList().get(i))) {
        long grouping = grouping(agg.getAggCallList().get(i), groupSet);
        projects.add(rexBuilder.makeBigintLiteral(BigDecimal.valueOf(grouping)));
      } else {
        projects.add(new RexInputRef(inputRef, agg.getRowType().getFieldList().get(aggGroupSetSize + i).getType()));
        inputRef++;
      }
    }
    relBuilder.project(projects);
  }

  private static long grouping(AggregateCall call, ImmutableBitSet groupSet) {
    long groupId = 0;
    int i = 0;
    do {
      groupId = groupId << 1;
      groupId = groupId + (groupSet.get(call.getArgList().get(i)) ? 0 : 1);
      i++;
    } while (i < call.getArgList().size());
    return groupId;
  }
}
