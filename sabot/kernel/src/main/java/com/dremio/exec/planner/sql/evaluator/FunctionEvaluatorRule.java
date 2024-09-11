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
package com.dremio.exec.planner.sql.evaluator;

import com.dremio.options.OptionResolver;
import com.dremio.sabot.exec.context.ContextInformation;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;

public final class FunctionEvaluatorRule extends RelRule<RelRule.Config> {
  private final ContextInformation contextInformation;
  private final OptionResolver optionResolver;

  public FunctionEvaluatorRule(
      ContextInformation contextInformation, OptionResolver optionResolver) {
    super(
        Config.EMPTY
            .withDescription("FunctionEvaluatorRule")
            .withOperandSupplier(op -> op.operand(RelNode.class).anyInputs()));
    this.contextInformation = contextInformation;
    this.optionResolver = optionResolver;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RelNode relNode = call.rel(0);
    RelNode rewrittenRelNode =
        FunctionEvaluatorUtil.evaluateAll(relNode, contextInformation, optionResolver);
    if (relNode != rewrittenRelNode) {
      call.transformTo(rewrittenRelNode);
    }
  }
}
