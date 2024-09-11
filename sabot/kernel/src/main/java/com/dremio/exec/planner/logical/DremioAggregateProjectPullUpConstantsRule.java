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

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

/**
 * Copied from AggregateProjectPullUpConstantsRule. Added removeAll option.
 *
 * <p>Planner rule that removes constant keys from an {@link org.apache.calcite.rel.core.Aggregate}.
 *
 * <p>Constant fields are deduced using {@link RelMetadataQuery#getPulledUpPredicates(RelNode)}; the
 * input does not need to be a {@link org.apache.calcite.rel.core.Project}.
 *
 * <p>By default, this rules never removes the last column, because {@code Aggregate([])} returns 1
 * row even if its input is empty.
 *
 * <p>When {@code this.removeAll} is set to true this rule will force removing the last column. A
 * filter will be added in this case to ensure {@code Aggregate([])} returns 0 row when its input is
 * empty.
 *
 * <p>Since the transformed relational expression has to match the original relational expression,
 * the constants are placed in a projection above the reduced aggregate. If those constants are not
 * used, another rule will remove them from the project.
 */
public class DremioAggregateProjectPullUpConstantsRule
    extends RelRule<DremioAggregateProjectPullUpConstantsRule.Config> {

  // ~ Constructors -----------------------------------------------------------

  protected DremioAggregateProjectPullUpConstantsRule(Config config) {
    super(config);
  }

  // ~ Methods ----------------------------------------------------------------

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    final RelNode input = aggregate.getInput();

    final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
    final RelMetadataQuery mq = call.getMetadataQuery();
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(aggregate.getInput());
    if (predicates == null) {
      return;
    }
    final NavigableMap<Integer, RexNode> map = new TreeMap<>();
    for (int key : aggregate.getGroupSet()) {
      final RexInputRef ref = rexBuilder.makeInputRef(aggregate.getInput(), key);
      if (predicates.constantMap.containsKey(ref)) {
        map.put(key, predicates.constantMap.get(ref));
      }
    }

    // None of the group expressions are constant. Nothing to do.
    if (map.isEmpty()) {
      return;
    }

    ImmutableBitSet newGroupSet = aggregate.getGroupSet();
    for (int key : map.keySet()) {
      newGroupSet = newGroupSet.clear(key);
    }
    final int newGroupCount = newGroupSet.cardinality();

    // If the constants are on the trailing edge of the group list, we just
    // reduce the group count.
    final RelBuilder relBuilder = call.builder();
    relBuilder.push(input);

    final int groupCount = aggregate.getGroupCount();

    // Clone aggregate calls.
    final List<AggregateCall> newAggCalls = new ArrayList<>();
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      newAggCalls.add(
          aggCall.adaptTo(
              input, aggCall.getArgList(), aggCall.filterArg, groupCount, newGroupCount));
    }

    // If all GROUP BY keys have been removed, add "HAVING COUNT(*) > 0" to ensure
    // "GROUP BY ()" returns 0 row when its input is empty.
    if (newGroupCount == 0) {
      // Add "COUNT(*)" aggregate function
      final RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
      newAggCalls.add(
          AggregateCall.create(
              SqlStdOperatorTable.COUNT,
              false,
              false,
              ImmutableIntList.of(),
              -1,
              RelCollations.EMPTY,
              typeFactory.createSqlType(SqlTypeName.BIGINT),
              null));
    }

    relBuilder.aggregate(relBuilder.groupKey(newGroupSet, null), newAggCalls);

    if (newGroupCount == 0) {
      // Add filter "> 0" on aggregate function "COUNT(*)"
      relBuilder.filter(
          relBuilder.call(
              SqlStdOperatorTable.GREATER_THAN,
              relBuilder.field(newAggCalls.size() - 1),
              relBuilder.literal(0)));
    }

    // Create a projection back again.
    List<Pair<RexNode, String>> projects = new ArrayList<>();
    int source = 0;
    for (RelDataTypeField field : aggregate.getRowType().getFieldList()) {
      RexNode expr;
      final int i = field.getIndex();
      if (i >= groupCount) {
        // Aggregate expressions' names and positions are unchanged.
        expr = relBuilder.field(i - map.size());
        // When newGroupCount is 0 the cloned aggregate calls (e.g. MAX)
        // may lose NOT NULL attribute.
        RelDataType originalType = field.getType();
        if (!originalType.equals(expr.getType())) {
          expr = rexBuilder.makeCast(originalType, expr, true);
        }
      } else {
        int pos = aggregate.getGroupSet().nth(i);
        if (map.containsKey(pos)) {
          expr = map.get(pos);
          RelDataType originalType = field.getType();
          if (!originalType.equals(expr.getType())) {
            expr = rexBuilder.makeCast(originalType, expr, true);
          }
        } else {
          expr = relBuilder.field(source++);
        }
      }
      projects.add(Pair.of(expr, field.getName()));
    }
    relBuilder.project(Pair.left(projects), Pair.right(projects)); // inverse
    call.transformTo(relBuilder.build());
  }

  public interface Config extends RelRule.Config {
    Config DEFAULT =
        RelRule.Config.EMPTY
            .withDescription("DremioAggregatePullUpAllConstantsRule")
            .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
            .withOperandSupplier(
                b0 -> b0.operand(LogicalAggregate.class).predicate(Aggregate::isSimple).anyInputs())
            .as(Config.class);

    @Override
    default DremioAggregateProjectPullUpConstantsRule toRule() {
      return new DremioAggregateProjectPullUpConstantsRule(this);
    }
  }
}

// End DremioAggregateProjectPullUpConstantsRule.java
