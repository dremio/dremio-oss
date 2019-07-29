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

import com.dremio.exec.calcite.logical.FlattenCrel;

/**
 * Converts FlattenCrel in Calcite's Convention.NONE to LOGICAL.
 */
public class FlattenRule extends RelOptRule {

  public static FlattenRule INSTANCE = new FlattenRule();

  private FlattenRule() {
    super(RelOptHelper.any(FlattenCrel.class, Convention.NONE), "FlattenRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FlattenCrel flattenRel = call.rel(0);
    final RelNode convertedInput = convert(flattenRel.getInput(), flattenRel.getInput().getTraitSet().plus(Rel.LOGICAL).simplify());
    call.transformTo(new FlattenRel(
        flattenRel.getCluster(),
        flattenRel.getInput().getTraitSet().plus(Rel.LOGICAL),
        convertedInput,
        flattenRel.getToFlatten(),
        flattenRel.getNumProjectsPushed()));
  }
}
