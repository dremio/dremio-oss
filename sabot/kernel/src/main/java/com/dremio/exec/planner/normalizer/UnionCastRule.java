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
package com.dremio.exec.planner.normalizer;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.sql.ConsistentTypeUtil;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;

/**
 * If we have a Union operation like:
 *
 * <p>UNION 1 2.0 3
 *
 * <p>Then the RelDataType will be DOUBLE, since it goes for the least restrictive type, but we need
 * to add CASTS to the input reps, since the physical operators, do not auto cast for us and we get
 * a schema mismatch exception like so:
 *
 * <p>Unable to complete query, attempting to union two datasets that have different underlying
 * schemas. Left: schema(col::decimal(12,11)), Right: schema(col::decimal(2,1))
 *
 * <p>This is a temp solution until the physical operator can handle mixed type data. O
 */
public final class UnionCastRule extends RelRule<RelRule.Config> {
  public static UnionCastRule INSTANCE = new UnionCastRule();

  private UnionCastRule() {
    super(
        Config.EMPTY
            .withDescription("UnionCastRule")
            .withOperandSupplier(op1 -> op1.operand(Union.class).anyInputs()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Union union = call.rel(0);
    List<RelDataType> childTypes =
        union.getInputs().stream().map(RelNode::getRowType).collect(Collectors.toList());
    RelDataType consistentType;
    if (ConsistentTypeUtil.allExactNumeric(childTypes)
        && ConsistentTypeUtil.anyDecimal(childTypes)) {
      consistentType =
          ConsistentTypeUtil.consistentDecimalType(union.getCluster().getTypeFactory(), childTypes);
    } else {
      consistentType =
          ConsistentTypeUtil.consistentType(
              union.getCluster().getTypeFactory(),
              SqlOperandTypeChecker.Consistency.LEAST_RESTRICTIVE,
              childTypes);
    }

    if (childTypes.stream().allMatch(type -> type == consistentType)) {
      return;
    }

    List<RelNode> convertedInputs =
        union.getInputs().stream()
            .map(input -> MoreRelOptUtil.createCastRel(input, consistentType))
            .collect(Collectors.toList());
    RelNode rewrittenUnion = union.copy(union.getTraitSet(), convertedInputs);
    call.transformTo(rewrittenUnion);
  }
}
