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

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.EQUALS;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.OR;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
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
 * Test for {@link MoreRelOptUtil}.
 */
public class TestMoreRelOptUtil {
  private static final RelDataTypeFactory typeFactory = SqlTypeFactoryImpl.INSTANCE;
  private static final RelDataType intColumnType = typeFactory.createTypeWithNullability(
    typeFactory.createSqlType(INTEGER), true);
  private static final RexBuilder rexBuilder = new DremioRexBuilder(typeFactory);
  private static final RelBuilder relBuilder = makeRelBuilder();

  private static final RexNode col_R_a = rexBuilder.makeInputRef(intColumnType,0);
  private static final RexNode col_R_b = rexBuilder.makeInputRef(intColumnType,1);
  private static final RexNode col_R_c = rexBuilder.makeInputRef(intColumnType,2);
  private static final RexNode col_R_d = rexBuilder.makeInputRef(intColumnType,3);
  private static final RexNode col_S_x = rexBuilder.makeInputRef(intColumnType,4);
  private static final RexNode col_S_y = rexBuilder.makeInputRef(intColumnType,5);
  private static final RexNode col_S_z = rexBuilder.makeInputRef(intColumnType,6);
  private static final RexNode col_S_w = rexBuilder.makeInputRef(intColumnType,7);

  private static final RexNode intLit10 = rexBuilder.makeLiteral(10,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit20 = rexBuilder.makeLiteral(20,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit30 = rexBuilder.makeLiteral(30,
    typeFactory.createSqlType(INTEGER), false);
  private static final RexNode intLit40 = rexBuilder.makeLiteral(40,
    typeFactory.createSqlType(INTEGER), false);

  @Test
  public void testShiftFilter() {
    Join joinRel = (Join) relBuilder
      .values(new String[] {"a", "b", "c", "d"}, 1, 2, 3, 4)
      .values(new String[] {"x", "y", "z", "w"}, 5, 6, 7, 8)
      .join(JoinRelType.INNER)
      .build();
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    List<RelDataTypeField> leftFields = joinRel.getInputs().get(0).getRowType().getFieldList();
    List<RelDataTypeField> rightFields = joinRel.getInputs().get(1).getRowType().getFieldList();

    RexNode leftFilter =
      rOr(
        rAnd(rEq(col_R_a, intLit10), rEq(col_R_b, intLit20)),
        rAnd(rEq(col_R_c, intLit30), rEq(col_R_d, intLit40)));
    RexNode rightFilter =
      rOr(
        rAnd(rEq(col_S_x, intLit10), rEq(col_S_y, intLit20)),
        rAnd(rEq(col_S_z, intLit30), rEq(col_S_w, intLit40)));

    RexNode leftFilterShifted = MoreRelOptUtil.shiftFilter(
      0,
      leftFields.size(),
      0,
      rexBuilder,
      joinFields,
      joinFields.size(),
      leftFields,
      leftFilter);
    RexNode rightFilterShifted = MoreRelOptUtil.shiftFilter(
      leftFields.size(),
      joinFields.size(),
      -leftFields.size(),
      rexBuilder,
      joinFields,
      joinFields.size(),
      rightFields,
      rightFilter);

    Assert.assertEquals("OR(AND(=($0, 10), =($1, 20)), AND(=($2, 30), =($3, 40)))",
      leftFilterShifted.toString());
    Assert.assertEquals("OR(AND(=($0, 10), =($1, 20)), AND(=($2, 30), =($3, 40)))",
      rightFilterShifted.toString());
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

  private static RelBuilder makeRelBuilder() {
    OptionResolver optionResolver = OptionResolverSpecBuilder.build(new OptionResolverSpec());
    PlannerSettings context = new PlannerSettings(null, optionResolver, null);
    RelOptPlanner planner = new HepPlanner(new HepProgramBuilder().build(), context, false,
      null, new DremioCost.Factory());
    RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
    return RelBuilder.proto(context).create(cluster, null);
  }
}
