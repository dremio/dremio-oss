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

import java.math.BigDecimal;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.planner.logical.RelOptHelper;


/**
 * Rule to convert {@link SamplePrel} to Dremio physical rel which is a {@link LimitPrel} in current implementation
 * of sampling. One difference is traits are not singleton.
 */
public class SampleToLimitPrule extends Prule {
  public static final RelOptRule INSTANCE = new SampleToLimitPrule();

  private SampleToLimitPrule() {
    super(RelOptHelper.any(SamplePrel.class, Prel.PHYSICAL), "Prel.SampleToLimitPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SamplePrel sample = (SamplePrel) call.rel(0);
    final RelNode input = sample.getInput();

    final RelNode convertedInput = convert(input, input.getTraitSet().plus(Prel.PHYSICAL));

    final RexBuilder rexBuilder = input.getCluster().getRexBuilder();
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(call.getPlanner());
    final RexNode offset = rexBuilder.makeBigintLiteral(BigDecimal.ZERO);
    final RexNode limit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(SampleCrel.getSampleSizeAndSetMinSampleSize(plannerSettings, 1)));
    final LimitPrel sampleAsLimit = new LimitPrel(sample.getCluster(), sample.getTraitSet().plus(Prel.PHYSICAL), convertedInput, offset, limit);
    call.transformTo(sampleAsLimit);
  }
}
