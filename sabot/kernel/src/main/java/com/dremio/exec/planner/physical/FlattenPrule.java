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
package com.dremio.exec.planner.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.planner.logical.FlattenRel;
import com.dremio.exec.planner.logical.RelOptHelper;

public class FlattenPrule extends Prule {

  public static final RelOptRule INSTANCE = new FlattenPrule();

  private FlattenPrule() {
    super(RelOptHelper.some(FlattenRel.class, RelOptHelper.any(RelNode.class)), "FlattenPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FlattenRel flatten = call.rel(0);
    final RelNode input = flatten.getInput();
    RelNode convertedInput = convert(input, input.getTraitSet().replace(Prel.PHYSICAL));

    FlattenPrel toTransform = null;
    for (RexNode rex : flatten.getToFlatten()) {
      toTransform = new FlattenPrel(flatten.getCluster(), convertedInput.getTraitSet(), convertedInput, rex);
      convertedInput = toTransform;
    }
    call.transformTo(toTransform);
  }
}
