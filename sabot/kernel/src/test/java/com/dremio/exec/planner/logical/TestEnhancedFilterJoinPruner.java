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
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
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
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  private static final RexNode col_R_a = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_R_c = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_S_x = rexBuilder.makeInputRef(intColumnType,3);

  // Shifted input refs
  private static final RexNode col_R_a_sh = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b_sh = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_R_c_sh = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_S_x_sh = rexBuilder.makeInputRef(intColumnType,0);

  private static final RexNode intLit10 = rexBuilder.makeLiteral(10,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 = rexBuilder.makeLiteral(30,
    typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testPruneSupersetInAndSubsetHasMultiChild() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_c, intLit30)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      true,
      "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasMultiChild() {
    testPruneSuperset(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_c, intLit30)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      true,
      "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInAndSubsetHasSingleChild() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_c, intLit30)),
        rEq(col_R_a, intLit10)),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInOrSubsetHasSingleChild() {
    testPruneSuperset(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_c, intLit30)),
        rEq(col_R_a, intLit10)),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetInAndSubsetSameAsSuperset() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      true,
      "OR(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetInOrSubsetSameAsSuperset() {
    testPruneSuperset(
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      true,
      "AND(=($0, 10), =($1, 20))");
  }

  @Test
  public void testPruneSupersetMultiSupersetRelationship() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_b, intLit30)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rEq(col_R_a, intLit10)),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetToLeaf() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_b, intLit30)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      true,
      "=($0, 10)");
  }

  @Test
  public void testPruneSupersetNotToLeaf() {
    testPruneSuperset(
      rAnd(
        rOr(rEq(col_R_a, intLit10), rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20), rEq(col_R_b, intLit30)),
        rOr(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20))),
      false,
      "AND(OR(=($0, 10), AND(=($0, 10), =($1, 20))), OR(=($0, 10), =($1, 20)))");
  }

  @Test
  public void testPrunePushdownJoinCondition() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      rexBuilder.makeLiteral(true));
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rEq(col_R_a, col_S_x);
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, joinRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownLeftFilter() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rEq(col_R_a_sh, intLit10),
      rexBuilder.makeLiteral(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rEq(col_R_a_sh, intLit10);
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownRightFilter() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rexBuilder.makeLiteral(true),
      rEq(col_S_x_sh, intLit10));
    RelNode rightRel = joinRel.getRight();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rEq(col_S_x_sh, intLit10);
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, rightRel, rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownNoPrune() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rEq(col_R_a_sh, intLit10),
      rexBuilder.makeLiteral(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rEq(col_R_b_sh, intLit20);
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownPartialPrune() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rEq(col_R_a_sh, intLit10),
      rexBuilder.makeLiteral(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rAnd(rEq(col_R_a_sh, intLit10), rEq(col_R_b_sh, intLit20));
    RexNode nodePruned = EnhancedFilterJoinPruner.prunePushdown(nodeToPrune, mq, leftRel, rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownExtractPrune() {
    Join joinRel = makeJoinRel(
      rEq(col_R_a, col_S_x),
      rOr(
        rAnd(rEq(col_R_a_sh, intLit10), rEq(col_R_b_sh, intLit10)),
        rAnd(rEq(col_R_a_sh, intLit20), rEq(col_R_c_sh, intLit20))),
      rexBuilder.makeLiteral(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = rOr(rEq(col_R_b_sh, intLit10), rEq(col_R_c_sh, intLit20));
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

  private static RexNode rEq(RexNode rexNode1, RexNode rexNode2) {
    return rexBuilder.makeCall(EQUALS, rexNode1, rexNode2);
  }

  private static RexNode rAnd(RexNode... rexNodes) {
    return rexBuilder.makeCall(AND, rexNodes);
  }

  private static RexNode rOr(RexNode... rexNodes) {
    return rexBuilder.makeCall(OR, rexNodes);
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
