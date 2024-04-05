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

import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

/** Converts dremio logical {@link EmptyRel} to dremio physical {@link EmptyPrel} */
public class EmptyPrule extends Prule {

  public static final RelOptRule INSTANCE = new EmptyPrule();

  public EmptyPrule() {
    super(RelOptHelper.any(EmptyRel.class), "Prel.EmptyPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final EmptyRel empty = call.rel(0);
    final RelTraitSet traits = empty.getTraitSet().plus(Prel.PHYSICAL);
    call.transformTo(
        new EmptyPrel(empty.getCluster(), traits, empty.getRowType(), empty.getSchema()));
  }
}
