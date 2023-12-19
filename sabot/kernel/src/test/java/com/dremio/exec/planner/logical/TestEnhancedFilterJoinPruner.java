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

import static com.dremio.test.dsl.RexDsl.and;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.intInput;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.or;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Assert;
import org.junit.Test;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;

/**
 * Test for {@link EnhancedFilterJoinPruner}.
 */
public class TestEnhancedFilterJoinPruner {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  @Test
  public void testPruneSupersetInAndSubsetHasMultiChild() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(2), literal(30))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      true,
      "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasMultiChild() {
    testPruneSuperset(
      or(
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(2), literal(30))),
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      true,
      "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInAndSubsetHasSingleChild() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(2), literal(30))),
        eq(intInput(0), literal(10))),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasSingleChild() {
    testPruneSuperset(
      or(
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(2), literal(30))),
        eq(intInput(0), literal(10))),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInAndSubsetSameAsSuperset() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      true,
      "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetSameAsSuperset() {
    testPruneSuperset(
      or(
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      true,
      "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetMultiSupersetRelationship() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(1), literal(30))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
        eq(intInput(0), literal(10))),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetToLeaf() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(1), literal(30))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetNotToLeaf() {
    testPruneSuperset(
      and(
        or(eq(intInput(0), literal(10)), and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)), eq(intInput(1), literal(30))),
        or(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)))),
      false,
      "AND(OR(=($0, 10), AND(=($0, 10), =($1, 20))), OR(=($0, 10), =($1, 20)))");
  }

  @Test
  public void testPrunePushdownJoinCondition() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      literal(true),
      literal(true));
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), intInput(3));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, joinRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownLeftFilter() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      eq(intInput(0), literal(10)),
      literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), literal(10));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownRightFilter() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      literal(true),
      eq(intInput(0), literal(10)));
    RelNode rightRel = joinRel.getRight();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), literal(10));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, rightRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownNoPrune() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      eq(intInput(0), literal(10)),
      literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(1), literal(20));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownPartialPrune() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      eq(intInput(0), literal(10)),
      literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownExtractPrune() {
    Join joinRel = makeJoinRel(
      eq(intInput(0), intInput(3)),
      or(
        and(eq(intInput(0), literal(10)), eq(intInput(1), literal(10))),
        and(eq(intInput(0), literal(20)), eq(intInput(2), literal(20)))),
      literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = or(eq(intInput(1), literal(10)), eq(intInput(2), literal(20)));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  private void testPruneSuperset(RexNode rexNode, boolean toLeaf, String expectedNodeString) {
    RexNode nodePruned = EnhancedFilterJoinPruner.pruneSuperset(rexBuilder, rexNode, toLeaf);
    Assert.assertEquals(expectedNodeString, nodePruned.toString());
  }

  private Join makeJoinRel(RexNode joinCondition, RexNode leftFilterShifted, RexNode rightFilterShifted) {
    return (Join) relBuilder
      .values(new String[] {"a", "b", "c"}, 1, 2, 3)
      .filter(leftFilterShifted)
      .values(new String[] {"x"}, 4)
      .filter(rightFilterShifted)
      .join(JoinRelType.INNER, joinCondition)
      .build();
  }

  private static org.apache.calcite.tools.RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build(), context, false,
      null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return RelBuilder.proto(context).create(cluster, null);
  }
}
