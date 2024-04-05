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

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalValues;

/** Rule that converts a {@link LogicalValues} to a Dremio "values" operation. */
public class ValuesRule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ValuesRule();

  private ValuesRule() {
    super(RelOptHelper.any(LogicalValues.class, Convention.NONE), "ValuesRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final LogicalValues values = call.rel(0);
    // Let the EmptyValuesRule handle this one.
    return !Values.isEmpty(values);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LogicalValues values = call.rel(0);
    final RelTraitSet traits = values.getTraitSet().plus(Rel.LOGICAL);
    call.transformTo(
        new ValuesRel(values.getCluster(), values.getRowType(), values.getTuples(), traits));
  }
}
