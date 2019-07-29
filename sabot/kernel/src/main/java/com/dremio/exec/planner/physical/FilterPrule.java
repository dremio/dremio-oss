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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.RelOptHelper;

public class FilterPrule extends Prule {
  public static final RelOptRule INSTANCE = new FilterPrule();

  private FilterPrule() {
    super(RelOptHelper.some(FilterRel.class, RelOptHelper.any(RelNode.class)), "FilterPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final FilterRel  filter = (FilterRel) call.rel(0);
    final RelNode input = filter.getInput();

    RelTraitSet traits = input.getTraitSet().plus(Prel.PHYSICAL);
    RelNode convertedInput = convert(input, traits);

    boolean transform = new Subset(call).go(filter, convertedInput);

    if (!transform) {
      call.transformTo(new FilterPrel(filter.getCluster(), convertedInput.getTraitSet(), convertedInput, filter.getCondition()));
    }
  }


  private class Subset extends SubsetTransformer<FilterRel, RuntimeException> {

    public Subset(RelOptRuleCall call) {
      super(call);
    }

    @Override
    public RelNode convertChild(FilterRel filter, RelNode rel) {
      return new FilterPrel(filter.getCluster(), rel.getTraitSet(), rel, filter.getCondition());
    }

  }
}
