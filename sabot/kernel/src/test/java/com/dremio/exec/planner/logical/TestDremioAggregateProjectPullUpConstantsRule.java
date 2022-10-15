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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NOT_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IS_NULL;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.PLUS;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelHintsPropagator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;

public class TestDremioAggregateProjectPullUpConstantsRule{
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  private static final RexNode col_R_c = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_R_d = rexBuilder.makeInputRef(intColumnType,3);

  private static final RexNode intLit1 = rexBuilder.makeLiteral(1,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);

  private static RelOptCluster cluster;

  private static final DremioAggregateProjectPullUpConstantsRule rule = DremioAggregateProjectPullUpConstantsRule.INSTANCE2_REMOVE_ALL;



  @Test
  public void testDremioAggregateProjectPullUpConstant() {

    RexNode equalCondFilter = rexBuilder.makeCall(EQUALS, col_R_c, intLit20);
    RexNode notNullCondFilter = rexBuilder.makeCall(IS_NOT_NULL, col_R_d);
    RexNode plusOneExprCondFilter = rexBuilder.makeCall(PLUS, col_R_d, intLit1);
    RexNode isNullCondFilter = rexBuilder.makeCall(IS_NULL, plusOneExprCondFilter);
    RexNode orCondFilter = rexBuilder.makeCall(OR, notNullCondFilter, isNullCondFilter);
    RexNode inputFilterCondition = rexBuilder.makeCall(AND, equalCondFilter, orCondFilter);

    Filter filter = (Filter) relBuilder
      .values(new String[]{"a", "b", "c", "d"},
                            1,  4,   20,  3,
                            2,  6,   20,  3)
      .filter(inputFilterCondition)
      .build();

    List<AggregateCall> aggCalls = new ArrayList<>();

    aggCalls.add(
      AggregateCall.create(SqlStdOperatorTable.COUNT, false,
        false, ImmutableIntList.of(), -1, RelCollations.EMPTY,
        typeFactory.createSqlType(SqlTypeName.BIGINT), null));

    Aggregate agg = (Aggregate) relBuilder
      .push(filter)
      .aggregate(relBuilder.groupKey(ImmutableBitSet.of(2, 3)), aggCalls).build();

    TestRelOptRuleCall testRelOptRuleCall = new TestRelOptRuleCall(cluster.getPlanner(),
      rule.getOperand(),
      new RelNode[]{agg, filter},
      null);

    rule.onMatch(testRelOptRuleCall);

    RelNode result = testRelOptRuleCall.outcome.get(0);

    Aggregate resultAgg = (Aggregate) result.getInput(0);

    Assert.assertEquals("{2, 3}", agg.getGroupSet().toString());
    Assert.assertEquals("{3}", resultAgg.getGroupSet().toString());

  }
  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build(), context, false,
      null, new DremioCost.Factory());
    cluster = RelOptCluster.create(planner, rexBuilder);
    return RelBuilder.proto(context).create(cluster, null);
  }
  static class TestRelOptRuleCall extends RelOptRuleCall {

    @SuppressWarnings("checkstyle:VisibilityModifier")
    final List<RelNode> outcome = new ArrayList<>();

    public TestRelOptRuleCall(RelOptPlanner planner, RelOptRuleOperand operand, RelNode[] rels,
                              Map<RelNode, List<RelNode>> nodeInputs) {
      super(planner, operand, rels, nodeInputs);
    }

    @Override
    public void transformTo(RelNode rel, Map<RelNode, RelNode> equiv, RelHintsPropagator handler) {
      outcome.add(rel);
    }

  }
}
