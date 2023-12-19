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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.physical.BridgeExchangePrel;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.Prule;

public class BridgeExchangePrule extends Prule {
  public static final RelOptRule INSTANCE = new BridgeExchangePrule();

  private BridgeExchangePrule() {
    super(RelOptHelper.any(BridgeExchangeRel.class, Rel.LOGICAL), "Prel.BridgeExchangePrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final BridgeExchangeRel exchange = call.rel(0);

    RelNode input = convert(exchange.getInput(), exchange.getTraitSet().replace(Prel.PHYSICAL));

    BridgeExchangePrel prel = new BridgeExchangePrel(
      exchange.getCluster(),
      exchange.getTraitSet().replace(Prel.PHYSICAL),
      input,
      exchange.getBridgeSetId()
    );
    call.transformTo(prel);
  }

}
