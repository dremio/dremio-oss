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
package com.dremio.exec.planner.rules;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * When we rewrite query plans we usually do so with the intent that the rule is going to optimize the query in some form.
 * Now there is always a possibility that the rule has a bug that leads to a runtime exception.
 * If we let the exception bubble up, then that leads to a bad experience for the user, since they now can't run a query that normally would have run without the optimization.
 * To avoid this experience this rule aims to "TryRewrite" the query and if it fails logs an error for asynchronous investigation.
 */
public final class OptimizationRelRuleWrapper extends RelOptRule {
  private static final Logger logger = LoggerFactory.getLogger(OptimizationRelRuleWrapper.class);

  private final RelOptRule innerRule;

  private OptimizationRelRuleWrapper(RelOptRule innerRule) {
    super(innerRule.getOperand());
    this.innerRule = Preconditions.checkNotNull(innerRule);
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    try {
      this.innerRule.onMatch(relOptRuleCall);
    } catch (Exception exception) {
      String relOptRuleInfo = relOptRuleCall.toString();
      String exceptionInfo = exception.toString();
      String derivedRuleName = this.getClass().getSimpleName();

      logger.warn(
          "RelOptRule: '{}' ran into the following exception: '{}' when trying to rewrite: '{}'.",
          relOptRuleInfo,
          exceptionInfo,
          derivedRuleName);
    }
  }

  @Override
  public boolean matches(RelOptRuleCall relOptRuleCall) {
    try {
      return this.innerRule.matches(relOptRuleCall);
    } catch (Exception exception) {
      String relOptRuleInfo = relOptRuleCall.toString();
      String exceptionInfo = exception.toString();
      String derivedRuleName = this.getClass().getSimpleName();

      logger.warn(
          "RelOptRule: '{}' ran into the following exception: '{}' when trying to match: '{}'.",
          relOptRuleInfo,
          exceptionInfo,
          derivedRuleName);
    }
    return false;
  }

  public static RelOptRule wrap(RelOptRule innerRule) {
    return new OptimizationRelRuleWrapper(innerRule);
  }
}
