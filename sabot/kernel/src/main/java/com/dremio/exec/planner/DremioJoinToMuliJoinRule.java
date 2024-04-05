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
package com.dremio.exec.planner;

import com.dremio.exec.planner.logical.DremioRelFactories;
import com.dremio.exec.planner.logical.JoinRel;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;

public class DremioJoinToMuliJoinRule extends JoinToMultiJoinRule {

  public static final JoinToMultiJoinRule INSTANCE = new DremioJoinToMuliJoinRule(true);

  public static final JoinToMultiJoinRule NO_OUTER = new DremioJoinToMuliJoinRule(false);
  private final boolean supportOuterJoin;

  protected DremioJoinToMuliJoinRule(boolean supportOuterJoin) {
    super(
        ((Config)
                JoinToMultiJoinRule.Config.DEFAULT
                    .withRelBuilderFactory(DremioRelFactories.LOGICAL_BUILDER)
                    .as(Config.class))
            .withOperandFor(JoinRel.class));
    this.supportOuterJoin = supportOuterJoin;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    JoinRel join = call.rel(0);
    return supportOuterJoin || !join.getJoinType().isOuterJoin();
  }
}
