/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.common.ScreenRelBase;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.ScreenRel;
import com.dremio.exec.planner.logical.RelOptHelper;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

public class ScreenPrule extends Prule{
  public static final RelOptRule INSTANCE = new ScreenPrule();


  public ScreenPrule() {
    super(RelOptHelper.some(ScreenRel.class, Rel.LOGICAL, RelOptHelper.any(RelNode.class)), "Prel.ScreenPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final ScreenRelBase screen = (ScreenRelBase) call.rel(0);
    final RelNode input = call.rel(1);

    final RelTraitSet traits = input.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);
    ScreenRelBase newScreen = new ScreenPrel(screen.getCluster(), screen.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON), convertedInput);
    call.transformTo(newScreen);
  }


}
