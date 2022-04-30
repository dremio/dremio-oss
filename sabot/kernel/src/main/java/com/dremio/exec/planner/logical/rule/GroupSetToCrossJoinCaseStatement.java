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

import static org.apache.calcite.sql.SqlKind.AVG_AGG_FUNCTIONS;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
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
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class GroupSetToCrossJoinCaseStatement {
  public static final int MAX_GROUPING_ARGS = 63; // number of bits in positive signed long

  public static final RelOptRule RULE = new RelOptRule(
      RelOptHelper.any(Aggregate.class, RelNode.class),
      "GroupSetToCrossJoinCaseStatement") {

    @Override
    public boolean matches(RelOptRuleCall call) {
      Aggregate agg = call.rel(0);
      // if we have an AVG functions, we want to make sure we run DremioAggregateReduceFunctionsRule first
      return agg.getAggCallList().stream().noneMatch(c -> c.getAggregation().getKind().belongsTo(AVG_AGG_FUNCTIONS));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      Aggregate agg = call.rel(0);
      RelOptCluster cluster = agg.getCluster();
      if(agg.groupSets.size() <= 1 && !containsGrouping(agg.getAggCallList())){
        return;
      }

      RelNode newAgg =
          new GroupSetToCrossJoinCaseStatement(cluster.getRexBuilder(), canPreAggregate(agg))
              .rewriteGroupSet(call::builder, agg);
      if(newAgg!= agg) {
        call.transformTo(newAgg);
      }
    }
  };

  private final RexBuilder rexBuilder;
  private final boolean preAggregate;
  private Mapping m;

  public GroupSetToCrossJoinCaseStatement(RexBuilder rexBuilder, boolean preAggregate) {
    this.rexBuilder = rexBuilder;
    this.preAggregate = preAggregate;
  }

  public RelNode rewriteGroupSet(Supplier<RelBuilder> relBuilderSupplier, Aggregate aggregate) {
    if(aggregate.groupSets.size() <= 1 && !containsGrouping(aggregate.getAggCallList())){
      return aggregate;
    }

    ImmutableList<ImmutableBitSet> groupSets = aggregate.groupSets;

    RelBuilder relBuilder =  relBuilderSupplier.get();
    relBuilder.push(aggregate.getInput());

    if (preAggregate) {
      relBuilder
        .aggregate(
          relBuilder.groupKey(aggregate.getGroupSet()),
          aggregate.getAggCallList().stream().filter(c -> !isGrouping(c)).collect(Collectors.toList()));
    }

    RelDataType inputType = relBuilder.peek().getRowType();

    return relBuilder
        .values(buildValuesRexNode(groupSets.size()), rowIntType())
        .join(JoinRelType.INNER, rexBuilder.makeLiteral(true))
        .project(buildProjectWithCaseStatementForGroupingsRexList(
            aggregate,
            inputType,
            aggregate.getGroupSets()))
        .aggregate(
            buildGroupKey(relBuilder, aggregate.getGroupSet(), inputType),
            transformAggCalls(aggregate.getAggCallList(), aggregate.getGroupCount()))
        .project(
          buildTopProject(aggregate),
          aggregate.getRowType().getFieldNames())
        .build();
  }

  public static boolean isGrouping(AggregateCall call) {
    return call.getAggregation().getKind() == SqlKind.GROUPING_ID || call.getAggregation().getKind() == SqlKind.GROUPING;
  }

  public static boolean containsGrouping(List<AggregateCall> callList) {
    for (AggregateCall c : callList) {
      if (isGrouping(c)) {
        return true;
      }
    }
    return false;
  }

  private List<AggregateCall> transformAggCalls(List<AggregateCall> calls, int offset) {
    List<AggregateCall> groupingRemoved = calls.stream().filter(c -> !isGrouping(c)).collect(Collectors.toList());
    return Ord.zip(groupingRemoved).stream()
      .filter(c -> !isGrouping(c.e))
      .map(call -> {
        AggregateCall c = call.e;
        SqlAggFunction func = c.getAggregation();
        if (preAggregate && c.getAggregation().getKind() == SqlKind.COUNT) {
          func = SqlStdOperatorTable.SUM0;
        }
        List<Integer> args;
        if (preAggregate) {
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
      .map(c -> c.transform(m))
      .collect(Collectors.toList());
  }

  private RexNode groupingRex(AggregateCall call, Aggregate aggRel, RelDataType groupIndexType) {
    if (call.getArgList().size() > MAX_GROUPING_ARGS) {
      throw UserException.validationError()
        .message("%d arguments exceeds maximum number of arguments %d for function %s",
          call.getArgList().size(), MAX_GROUPING_ARGS, call.getAggregation().getName())
        .buildSilently();
    }
    final RexBuilder rexBuilder = aggRel.getCluster().getRexBuilder();
    final int groupIndexIndex = aggRel.getGroupCount();
    List<RexNode> exprs = new ArrayList<>();
    for (Ord<ImmutableBitSet> groupSet : Ord.zip(aggRel.getGroupSets())) {
      RexNode equals = rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(groupIndexType, groupIndexIndex),
        rexBuilder.makeBigintLiteral(BigDecimal.valueOf(groupSet.i)));
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
    // the default condition shouldn't ever happen, but it's needed to preserve the non-null type of the
    // case statement
    exprs.add(rexBuilder.makeBigintLiteral(BigDecimal.ONE));
    return rexBuilder.makeCall(SqlStdOperatorTable.CASE, exprs);
  }

  private List<RexNode> buildTopProject(Aggregate aggregate) {
    ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
    ImmutableBitSet groupSet = aggregate.getGroupSet();
    List<RelDataTypeField> aggRowType = aggregate.getRowType().getFieldList();
    IntStream.range(0, groupSet.cardinality())
        .mapToObj(index -> rexBuilder.makeInputRef(aggRowType.get(index).getType(), index))
        .forEach(builder::add);
    int idx = groupSet.cardinality() + 1;
    for (AggregateCall call : aggregate.getAggCallList()) {
      if (isGrouping(call)) {
        builder.add(groupingRex(call, aggregate, intType()));
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
      RelDataType inputType) {
    return relBuilder.groupKey(
        ImmutableBitSet.builder()
            .addAll(allColumnsInGroupings)
            .set(inputType.getFieldCount())
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
      Aggregate agg,
      RelDataType inputType,
      ImmutableList<ImmutableBitSet> groupingSets
  ) {
    ImmutableBitSet allColumnsInGroupings = agg.getGroupSet();
    int fieldCount = inputType.getFieldCount();
    RexNode groupSetIdxRef = rexBuilder.makeInputRef(intType(), fieldCount);


    Stream<RexNode> rexNodes = IntStream.range(0,fieldCount)
      .mapToObj(columnIndex ->
          createCaseStatement(
              inputType,
              groupSetIdxRef,
              columnIndex,
              allColumnsInGroupings,
              groupingSets)

          );
    Set<Integer> aggInputsToDuplicate = new TreeSet<>();

    if (!preAggregate) {
      for (AggregateCall call : agg.getAggCallList()) {
        if (isGrouping(call)) {
          continue;
        }
        for (int i : call.getArgList()) {
          if (allColumnsInGroupings.get(i)) {
            aggInputsToDuplicate.add(i);
          }
        }
      }

      m = Mappings.create(MappingType.FUNCTION, fieldCount, fieldCount + 1 + aggInputsToDuplicate.size());
      for (int i = 0; i < fieldCount; i++) {
        m.set(i, i);
      }
      int idx = fieldCount + 1;
      for (int i : aggInputsToDuplicate) {
        m.set(i, idx++);
      }
    } else {
      m = Mappings.createIdentity(fieldCount);
    }

    return Stream.concat(
        Stream.concat(
        rexNodes,
        Stream.of(groupSetIdxRef)
        ),
        aggInputsToDuplicate.stream().map(i -> new RexInputRef(i, inputType.getFieldList().get(i).getType())))
        .collect(ImmutableList.toImmutableList());
  }

  private RexNode createCaseStatement(
      RelDataType inputType,
      RexNode currentColumnIndexRef,
      int columnIndex,
      ImmutableBitSet allColumnsInGroupings,
      ImmutableList<ImmutableBitSet> groupingSets) {
    RexInputRef inputRef = rexBuilder.makeInputRef(inputType.getFieldList().get(columnIndex).getType(), columnIndex);
    if(!allColumnsInGroupings.get(columnIndex)) {
      return inputRef;
    }

    RexLiteral nullLiteral = rexBuilder.makeNullLiteral(inputRef.getType());
    List<RexNode> groupingSetOrStatements = groupingSets.stream()
      .filter(gs -> gs.get(columnIndex))
      .map(groupingSets::indexOf)
      .map(groupSetIndex ->
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            currentColumnIndexRef,
            makeLiteral(groupSetIndex))
      )
      .collect(Collectors.toList());
    RexNode condition;
    if (groupingSetOrStatements.size() > 1) {
      condition = rexBuilder.makeCall(SqlStdOperatorTable.OR, groupingSetOrStatements);
    } else if (groupingSetOrStatements.size() == 1) {
      condition = groupingSetOrStatements.get(0);
    } else {
      condition = rexBuilder.makeLiteral(false);
    }
    return rexBuilder.makeCall(
      SqlStdOperatorTable.CASE,
      condition,
      inputRef,
      nullLiteral
    );
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

  private static final Set<SqlKind> PRE_AGGREGATION_FUNCTIONS = ImmutableSet.of(
    SqlKind.SUM,
    SqlKind.SUM0,
    SqlKind.COUNT,
    SqlKind.MIN,
    SqlKind.MAX,
    SqlKind.GROUP_ID,
    SqlKind.GROUPING
  );

  private static boolean canPreAggregate(Aggregate agg) {
    return agg.getAggCallList().stream()
      .allMatch(a -> PRE_AGGREGATION_FUNCTIONS.contains(a.getAggregation().getKind()));
  }
}
