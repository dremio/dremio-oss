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

import com.dremio.exec.planner.acceleration.ExpansionNode;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

/** Remove the ExpansionNode when converting to logical/drel land. */
public class ExpansionDrule extends RelOptRule {
  public static final RelOptRule INSTANCE = new ExpansionDrule();

  private ExpansionDrule() {
    super(
        RelOptHelper.some(ExpansionNode.class, Convention.NONE, RelOptHelper.any(RelNode.class)),
        "ExpansionRemovalRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final RelNode input = call.rel(1);
    call.transformTo(convert(input, input.getTraitSet().plus(Rel.LOGICAL).simplify()));
  }
}
