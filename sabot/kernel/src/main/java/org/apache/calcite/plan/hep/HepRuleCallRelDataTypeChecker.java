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
package org.apache.calcite.plan.hep;

import com.dremio.exec.catalog.udf.TabularUserDefinedFunctionExpanderRule;
import com.dremio.exec.planner.logical.MergeProjectRule;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
import com.dremio.exec.planner.logical.PushProjectIntoScanRule;
import com.dremio.exec.planner.logical.RelDataTypeEqualityComparer;
import com.dremio.exec.planner.logical.RewriteProjectToFlattenRule;
import com.dremio.exec.planner.logical.RollupWithBridgeExchangeRule;
import com.dremio.exec.planner.normalizer.DremioAggregateRemoveRule;
import com.dremio.exec.planner.normalizer.DremioArraySubQueryRemoveRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.CollectToArrayAggRule;
import com.dremio.exec.planner.normalizer.aggregaterewrite.PercentileFunctionsRewriteRule;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.MultiJoinOptimizeBushyRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.type.RelDataType;

/** Helps assert that planner rules preserve RelDataType across rewrites. */
public final class HepRuleCallRelDataTypeChecker {
  private HepRuleCallRelDataTypeChecker() {}

  // Rules that don't preserve the RelDateType can be bypassed until a fix can be prepared. Rules
  // should not be added to this list. It is only meant to bypass current rules until they can be
  // fixed.
  private static final List<Class<?>> WHITELIST =
      ImmutableList.of(
          ProjectRemoveRule.class,
          AggregateProjectMergeRule.class,
          MultiJoinOptimizeBushyRule.class,
          AggregateJoinTransposeRule.class,
          RollupWithBridgeExchangeRule.class,
          ProjectToWindowRule.class,
          ProjectMergeRule.class,
          ProjectJoinTransposeRule.class,
          MergeProjectRule.class,
          RewriteProjectToFlattenRule.class,
          ProjectFilterTransposeRule.class,
          PercentileFunctionsRewriteRule.class,
          JoinPushExpressionsRule.class,
          PushFilterPastProjectRule.class,
          PushProjectIntoScanRule.class,
          CollectToArrayAggRule.class,
          DremioArraySubQueryRemoveRule.class,
          ProjectSetOpTransposeRule.class,
          TabularUserDefinedFunctionExpanderRule.class,
          DremioAggregateRemoveRule.class,
          ProjectWindowTransposeRule.class);

  public static List<Mismatch> getMismatches(HepRuleCall hepRuleCall) {
    RelNode originalRelNode = hepRuleCall.rel(0);
    if (skipRule(hepRuleCall.getRule())) {
      return ImmutableList.of();
    }

    List<Mismatch> mismatches =
        hepRuleCall.getResults().stream()
            .filter(
                transformedRelNode ->
                    !RelDataTypeEqualityComparer.areEqual(
                        originalRelNode.getRowType(), transformedRelNode.getRowType()))
            .map(
                transformedRelNode ->
                    new Mismatch(
                        hepRuleCall.getRule(),
                        originalRelNode.getRowType(),
                        transformedRelNode.getRowType()))
            .collect(Collectors.toList());
    return mismatches;
  }

  private static boolean skipRule(RelOptRule rule) {
    for (Class<?> ruleClass : WHITELIST) {
      if (ruleClass.isAssignableFrom(rule.getClass())) {
        return true;
      }
    }
    return false;
  }

  public static final class Mismatch {
    private final RelOptRule violatingRule;
    private final RelDataType originalRelDataType;
    private final RelDataType transformedRelDataType;

    public Mismatch(
        RelOptRule violatingRule,
        RelDataType originalRelDataType,
        RelDataType transformedRelDataType) {
      this.violatingRule = violatingRule;
      this.originalRelDataType = originalRelDataType;
      this.transformedRelDataType = transformedRelDataType;
    }

    public RelOptRule getViolatingRule() {
      return violatingRule;
    }

    public RelDataType getOriginalRelDataType() {
      return originalRelDataType;
    }

    public RelDataType getTransformedRelDataType() {
      return transformedRelDataType;
    }
  }
}
