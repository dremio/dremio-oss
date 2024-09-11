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
package com.dremio.exec.planner.rules;

import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.JoinRel;
import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;

public class SimpleAggJoinTransposeRule extends AggregateJoinTransposeRule {

  private static final Set<SqlKind> UNSUPPORTED_DECIMAL_AGGS =
      ImmutableSet.of(SqlKind.SUM, SqlKind.SUM0);

  protected SimpleAggJoinTransposeRule() {
    super(AggregateRel.class, JoinRel.class, DremioRelFactories.LOGICAL_BUILDER, true);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    AggregateRel agg = call.rel(0);
    if (!Aggregate.isSimple(agg)) {
      return false;
    }
    if (agg.getAggCallList().stream()
        .anyMatch(
            c ->
                UNSUPPORTED_DECIMAL_AGGS.contains(c.getAggregation().getKind())
                    && !supportedDecimalType(c.getType()))) {
      return false;
    }
    JoinRel join = call.rel(1);
    ImmutableBitSet leftKeys = ImmutableBitSet.of(join.getLeftKeys());
    ImmutableBitSet rightKeys =
        ImmutableBitSet.of(join.getRightKeys()).shift(join.getLeft().getRowType().getFieldCount());
    if (leftKeys.cardinality() < 1) {
      return false;
    }
    return Pair.zip(leftKeys.asList(), rightKeys.asList()).stream()
        .allMatch(p -> agg.getGroupSet().get(p.left) || agg.getGroupSet().get(p.right));
  }

  private static boolean supportedDecimalType(RelDataType type) {
    if (type.getSqlTypeName() != SqlTypeName.DECIMAL) {
      return true;
    }
    // due to dremio's decimal scale/precision rules, higher precision than 6 will lose precision
    // when
    // doing this push down
    return type.getScale() <= 6;
  }
}
