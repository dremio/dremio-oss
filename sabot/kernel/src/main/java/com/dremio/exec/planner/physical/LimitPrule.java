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

import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

public class LimitPrule extends Prule{
  public static final RelOptRule INSTANCE = new LimitPrule();

  private LimitPrule() {
    super(RelOptHelper.any(LimitRel.class, Rel.LOGICAL), "Prel.LimitPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LimitRel limit = (LimitRel) call.rel(0);
    final RelNode input = limit.getInput();

    final RelTraitSet traits = input.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON);
    final RelNode convertedInput = convert(input, traits);
    LimitPrel newLimit = new LimitPrel(limit.getCluster(), limit.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON), convertedInput, limit.getOffset(), limit.getFetch());
    call.transformTo(newLimit);
  }
}
