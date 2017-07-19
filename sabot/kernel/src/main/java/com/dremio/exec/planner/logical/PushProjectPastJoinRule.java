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
package com.dremio.exec.planner.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.PushProjector;
import org.apache.calcite.rel.rules.PushProjector.ExprCondition;
import org.apache.calcite.tools.RelBuilderFactory;

public class PushProjectPastJoinRule extends ProjectJoinTransposeRule {

  public static final RelOptRule CALCITE_INSTANCE = new PushProjectPastJoinRule(
    ExprCondition.TRUE,
    DremioRelFactories.CALCITE_LOGICAL_BUILDER,
    Convention.NONE);

  public static final RelOptRule INSTANCE = new PushProjectPastJoinRule(
    ExprCondition.TRUE,
    DremioRelFactories.LOGICAL_BUILDER,
    Rel.LOGICAL);

  private final Convention convention;

  protected PushProjectPastJoinRule(PushProjector.ExprCondition preserveExprCondition,
                                    RelBuilderFactory relFactory,
                                    Convention convention) {
    super(preserveExprCondition, relFactory);
    this.convention = convention;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0).getConvention() == call.rel(1).getConvention()
        && call.rel(0).getConvention() == convention;
  }

}
