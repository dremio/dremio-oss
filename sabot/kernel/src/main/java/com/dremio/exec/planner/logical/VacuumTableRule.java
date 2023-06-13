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
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.calcite.logical.VacuumTableCrel;

/**
 * Planner rule, Applicable for VACUUM command only.
 */
public class VacuumTableRule extends RelOptRule {

  public static final RelOptRule INSTANCE = new VacuumTableRule();

  private VacuumTableRule() {
    super(RelOptHelper.any(VacuumTableCrel.class, Convention.NONE), "VacuumTableRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final VacuumTableCrel vacuumTableCrel = call.rel(0);
    final RelNode input = vacuumTableCrel.getInput();
    //Set traits with Logical convention, Else it won't fetch the best plan.
    final RelNode convertedInput = convert(input, input.getTraitSet().plus(Rel.LOGICAL).simplify());
    call.transformTo(new VacuumTableRel(
      vacuumTableCrel.getCluster(),
      vacuumTableCrel.getTraitSet().plus(Rel.LOGICAL),
      convertedInput,
      vacuumTableCrel.getTable(),
      vacuumTableCrel.getCreateTableEntry(),
      vacuumTableCrel.getVacuumOptions()));
  }
}
