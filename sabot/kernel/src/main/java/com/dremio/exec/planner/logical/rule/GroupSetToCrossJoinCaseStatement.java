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
package com.dremio.exec.planner.logical.rule;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.google.common.collect.ImmutableList;

public class GroupSetToCrossJoinCaseStatement {
  public static final RelOptRule RULE = new RelOptRule(
      RelOptHelper.any(Aggregate.class, RelNode.class),
      "GroupSetToCrossJoinCaseStatement") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      Aggregate agg = call.rel(0);
      RelOptCluster cluster = agg.getCluster();
      if(agg.groupSets.size() <= 1 && !containsGrouping(agg.getAggCallList())){
        return;
      }

      RelNode newAgg =
          new GroupSetToCrossJoinCaseStatement(cluster.getRexBuilder())
              .rewriteGroupSet(call::builder, agg, call.rel(1));
      if(newAgg!= agg) {
        call.transformTo(newAgg);
      }
    }
  };

  private final RexBuilder rexBuilder;

  public GroupSetToCrossJoinCaseStatement(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
  }

  public RelNode rewriteGroupSet(Supplier<RelBuilder> relBuilderSupplier, Aggregate aggregate, RelNode relNode) {
    ImmutableList<ImmutableBitSet> groupSets = aggregate.groupSets;
    if(aggregate.groupSets.size() <= 1 && !containsGrouping(aggregate.getAggCallList())){
      return aggregate;
    }

    RelBuilder relBuilder =  relBuilderSupplier.get();
    return relBuilder
        .push(relNode)
        .values(buildValuesRexNode(groupSets.size()), rowIntType())
        .join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
        .project(buildProjectWithCaseStatementForGroupingsRexList(
            relNode,
            aggregate.getGroupSet(),
            groupSets))
        .aggregate(
            buildGroupKey(relBuilder, aggregate.getGroupSet(), relNode),
            aggregate.getAggCallList().stream().filter(c -> !isGrouping(c)).collect(Collectors.toList()))
        .project(buildTopProject(aggregate, relBuilder.peek().getRowType().getFieldList().get(aggregate.getGroupCount()).getType()), aggregate.getRowType().getFieldNames())
        .build();
  }

  public static boolean isGrouping(AggregateCall call) {
    return call.getAggregation().getKind() == SqlKind.GROUP_ID || call.getAggregation().getKind() == SqlKind.GROUPING;
  }

  public static boolean containsGrouping(List<AggregateCall> callList) {
    for (AggregateCall c : callList) {
      if (isGrouping(c)) {
        return true;
      }
    }
    return false;
  }

  private RexNode groupingRex(AggregateCall call, Aggregate aggRel, RelDataType groupIndexType) {
    final RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();
    final int groupIndexIndex = aggRel.getGroupCount();
    List<RexNode> exprs = new ArrayList<>();
    for (Ord<ImmutableBitSet> groupSet : Ord.zip(aggRel.getGroupSets())) {
      RexNode equals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(groupIndexType, groupIndexIndex), rexBuilder.makeBigintLiteral(BigDecimal.valueOf(groupSet.i)));
      int groupId = 0;
      int i = 0;
      do {
        groupId = groupId << 1;
        groupId = groupId + (groupSet.e.get(call.getArgList().get(i)) ? 0 : 1);
        i++;
      } while (i < call.getArgList().size());
      RexNode value = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(groupId));
      exprs.add(equals);
      exprs.add(value);
    }
    exprs.add(rexBuilder.makeBigintLiteral(BigDecimal.ONE));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, exprs);
  }

  private List<RexNode> buildTopProject(Aggregate aggregate, RelDataType groupIndexType) {
    ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    ImmutableBitSet groupSet = aggregate.getGroupSet();
    List<RelDataTypeField> aggRowType = aggregate.getRowType().getFieldList();
    IntStream.range(0, groupSet.cardinality())
        .mapToObj(index -> rexBuilder.makeInputRef(aggRowType.get(index).getType(), index))
        .forEach(builder::add);
    int idx = groupSet.cardinality() + 1;
    for (AggregateCall call : aggregate.getAggCallList()) {
      if (isGrouping(call)) {
        builder.add(groupingRex(call, aggregate, groupIndexType));
      } else {
        builder.add(rexBuilder.makeInputRef(call.getType(), idx));
        idx++;
      }
    }
    return builder.build();
  }

  private RelBuilder.GroupKey buildGroupKey(
      RelBuilder relBuilder,
      ImmutableBitSet allColumnsInGroupings,
      RelNode relNode) {
    return relBuilder.groupKey(
        ImmutableBitSet.builder()
            .addAll(allColumnsInGroupings)
            .set(relNode.getRowType().getFieldCount())
            .build(),
        null);
  }

  private Iterable<? extends List<RexLiteral>> buildValuesRexNode(int size) {
     return IntStream.range(0, size)
        .mapToObj(this::makeLiteral)
        .map(RexLiteral.class::cast)
        .map(ImmutableList::of)
        .collect(ImmutableList.toImmutableList());
  }

  private List<RexNode> buildProjectWithCaseStatementForGroupingsRexList(
      RelNode baseNode,
      ImmutableBitSet allColumnsInGroupings,
      ImmutableList<ImmutableBitSet> groupingSets
  ) {
    int fieldCount = baseNode.getRowType().getFieldCount();
    RexNode currentColumnIndexRef = rexBuilder.makeInputRef(intType(), fieldCount);


    Stream<RexNode> rexNodes = IntStream.range(0,fieldCount)
      .mapToObj(columnIndex ->
          createCaseStatement(
              baseNode,
              currentColumnIndexRef,
              columnIndex,
              allColumnsInGroupings,
              groupingSets)

          );
    return Stream.concat(
        rexNodes,
        Stream.of(currentColumnIndexRef))
        .collect(ImmutableList.toImmutableList());
  }

  private RexNode createCaseStatement(
      RelNode baseNode,
      RexNode currentColumnIndexRef,
      int columnIndex,
      ImmutableBitSet allColumnsInGroupings,
      ImmutableList<ImmutableBitSet> groupingSets) {
    RexInputRef inputRef = rexBuilder.makeInputRef(baseNode, columnIndex);
    if(!allColumnsInGroupings.get(columnIndex)) {
      return inputRef;
    }

    RexLiteral nullLiteral = rexBuilder.makeNullLiteral(inputRef.getType());
    Stream<RexNode> groupingSetCaseStatements =
        groupingSets.stream()
            .filter(gs -> gs.get(columnIndex))
            .map(groupingSets::indexOf)
            .flatMap(groupSetIndex ->
                Stream.of(
                    rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                        currentColumnIndexRef,
                        makeLiteral(groupSetIndex)),
                    inputRef));
    return rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        Stream.concat(
            groupingSetCaseStatements,
            Stream.of(nullLiteral))
          .collect(ImmutableList.toImmutableList()));
  }

  private RexNode makeLiteral(int value) {
    return rexBuilder.makeLiteral(
        value,
        intType(),
        false);
  }

  private RelDataType intType() {
    return rexBuilder.getTypeFactory().createSqlType(SqlTypeName.INTEGER);
  }

  private RelDataType rowIntType() {
    return rexBuilder.getTypeFactory().createStructType(
        ImmutableList.of(intType()),
        ImmutableList.of("group_set_index"));
  }


}
