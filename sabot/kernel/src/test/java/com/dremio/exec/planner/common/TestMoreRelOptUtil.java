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
package com.dremio.exec.planner.common;

import static com.dremio.test.dsl.RexDsl.and;
import static com.dremio.test.dsl.RexDsl.eq;
import static com.dremio.test.dsl.RexDsl.floatNullInput;
import static com.dremio.test.dsl.RexDsl.intInput;
import static com.dremio.test.dsl.RexDsl.intNullInput;
import static com.dremio.test.dsl.RexDsl.literal;
import static com.dremio.test.dsl.RexDsl.or;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import com.dremio.exec.planner.DremioRexBuilder;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.types.SqlTypeFactoryImpl;
import com.dremio.options.OptionResolver;
import com.dremio.test.specs.OptionResolverSpec;
import com.dremio.test.specs.OptionResolverSpecBuilder;
import com.google.common.collect.ImmutableList;
import java.util.List;
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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Assert;
import org.junit.Test;

/** Test for {@link MoreRelOptUtil}. */
public class TestMoreRelOptUtil {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType =
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(INTEGER), true);
  private static final RelDataType floatColumnType =
      typeFactory.createTypeWithNullability(typeFactory.createSqlType(FLOAT), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  @Test
  public void testIdentityProjects() {
    RelDataType rowType =
        typeFactory.createStructType(
            ImmutableList.of(intColumnType, floatColumnType), ImmutableList.of("c1", "c2"));
    Assert.assertEquals(
        ImmutableList.of(intNullInput(0), floatNullInput(1)),
        MoreRelOptUtil.identityProjects(rowType));
    Assert.assertEquals(
        ImmutableList.of(intNullInput(0), floatNullInput(1)),
        MoreRelOptUtil.identityProjects(rowType, ImmutableBitSet.of(0, 1)));
    Assert.assertEquals(
        ImmutableList.of(intNullInput(0)),
        MoreRelOptUtil.identityProjects(rowType, ImmutableBitSet.of(0)));
    Assert.assertEquals(
        ImmutableList.of(floatNullInput(1)),
        MoreRelOptUtil.identityProjects(rowType, ImmutableBitSet.of(1)));
  }

  @Test
  public void testShiftFilter() {
    Join joinRel =
        (Join)
            relBuilder
                .values(new String[] {"a", "b", "c", "d"}, 1, 2, 3, 4)
                .values(new String[] {"x", "y", "z", "w"}, 5, 6, 7, 8)
                .join(JoinRelType.INNER)
                .build();
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
    List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();

    RexNode leftFilter =
        or(
            and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20))),
            and(eq(intInput(2), literal(30)), eq(intInput(3), literal(40))));
    RexNode rightFilter =
        or(
            and(eq(intInput(4), literal(10)), eq(intInput(5), literal(20))),
            and(eq(intInput(6), literal(30)), eq(intInput(7), literal(40))));

    RexNode leftFilterShifted =
        MoreRelOptUtil.shiftFilter(
            0,
            leftFields.size(),
            0,
            rexBuilder,
            joinFields,
            joinFields.size(),
            leftFields,
            leftFilter);
    RexNode rightFilterShifted =
        MoreRelOptUtil.shiftFilter(
            leftFields.size(),
            joinFields.size(),
            -leftFields.size(),
            rexBuilder,
            joinFields,
            joinFields.size(),
            rightFields,
            rightFilter);

    Assert.assertEquals(
        "OR(AND(=($0, 10), =($1, 20)), AND(=($2, 30), =($3, 40)))", leftFilterShifted.toString());
    Assert.assertEquals(
        "OR(AND(=($0, 10), =($1, 20)), AND(=($2, 30), =($3, 40)))", rightFilterShifted.toString());
  }

  @Test
  public void testPrunePushdownJoinCondition() {
    Join joinRel = makeJoinRel(eq(intInput(0), intInput(3)), literal(true), literal(true));
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), intInput(3));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(joinRel), rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownLeftFilter() {
    Join joinRel =
        makeJoinRel(eq(intInput(0), intInput(3)), eq(intInput(0), literal(10)), literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), literal(10));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(leftRel), rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownRightFilter() {
    Join joinRel =
        makeJoinRel(eq(intInput(0), intInput(3)), literal(true), eq(intInput(0), literal(10)));
    RelNode rightRel = joinRel.getRight();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(0), literal(10));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(rightRel), rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownNoPrune() {
    Join joinRel =
        makeJoinRel(eq(intInput(0), intInput(3)), eq(intInput(0), literal(10)), literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = eq(intInput(1), literal(20));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(leftRel), rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownPartialPrune() {
    Join joinRel =
        makeJoinRel(eq(intInput(0), intInput(3)), eq(intInput(0), literal(10)), literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = and(eq(intInput(0), literal(10)), eq(intInput(1), literal(20)));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(leftRel), rexBuilder);
    Assert.assertEquals("=($1, 20)", nodePruned.toString());
  }

  @Test
  public void testPrunePushdownExtractPrune() {
    Join joinRel =
        makeJoinRel(
            eq(intInput(0), intInput(3)),
            or(
                and(eq(intInput(0), literal(10)), eq(intInput(1), literal(10))),
                and(eq(intInput(0), literal(20)), eq(intInput(2), literal(20)))),
            literal(true));
    RelNode leftRel = joinRel.getLeft();
    RelMetadataQuery mq = joinRel.getCluster().getMetadataQuery();

    RexNode nodeToPrune = or(eq(intInput(1), literal(10)), eq(intInput(2), literal(20)));
    RexNode nodePruned =
        MoreRelOptUtil.prunePushdown(nodeToPrune, mq.getPulledUpPredicates(leftRel), rexBuilder);
    Assert.assertEquals("true", nodePruned.toString());
  }

  private Join makeJoinRel(
      RexNode joinCondition, RexNode leftFilterShifted, RexNode rightFilterShifted) {
    return (Join)
        relBuilder
            .values(new String[] {"a", "b", "c"}, 1, 2, 3)
            .filter(leftFilterShifted)
            .values(new String[] {"x"}, 4)
            .filter(rightFilterShifted)
            .join(JoinRelType.INNER, joinCondition)
            .build();
  }

  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner =
        new HepPlanner(
            new HepProgramBuilder().build(), context, false, null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return RelBuilder.proto(context).create(cluster, null);
  }
}
