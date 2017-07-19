/*
 * Copyright (C) 2017 Dremio Corporation
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

import com.dremio.exec.planner.logical.SampleRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;

/**
 * Rule to convert {@link SampleRel} to Dremio physical rel which is a {@link SamplePrel}.
 */
public class SamplePrule extends Prule {
  public static final RelOptRule INSTANCE = new SamplePrule();

  private SamplePrule() {
    super(RelOptHelper.any(SampleRel.class, Rel.LOGICAL), "Prel.SamplePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SampleRel sample = (SampleRel) call.rel(0);
    final RelNode input = sample.getInput();

    final RelNode convertedInput = convert(input, input.getTraitSet().plus(Prel.PHYSICAL));

    final SamplePrel samplePrel = new SamplePrel(sample.getCluster(), sample.getTraitSet().plus(Prel.PHYSICAL), convertedInput);
    call.transformTo(samplePrel);
  }
}
