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

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;

/** Helps detect when planner rules generate no-op transformations. */
public class HepRuleCallNoOpDetector {

  // Rules that violate the equality checks below can be bypassed until a fix can be prepared. Rules
  // should not be added to this list. It is only meant to bypass current rules until they can be
  // fixed.
  private static final List<Class<?>> WHITELIST =
      ImmutableList.of(
              // reference equality skip rules
              "com.dremio.exec.planner.logical.rule.DremioReduceExpressionsRule",
              // object equality skip rules
              "com.dremio.exec.planner.normalizer.aggregaterewrite.AggregateCallRewriteRule",
              "org.apache.calcite.rel.rules.AggregateReduceFunctionsRule",
              "com.dremio.exec.planner.logical.PushFilterPastFlattenrule",
              "com.dremio.exec.planner.logical.RemoveEmptyScansRule",
              "com.dremio.exec.planner.normalizer.UnionCastRule")
          .stream()
          .map(
              rule -> {
                try {
                  return Class.forName(rule);
                } catch (ClassNotFoundException ignored) {
                  return null;
                }
              })
          .filter(Objects::nonNull)
          .collect(ImmutableList.toImmutableList());

  public static boolean hasNoOpTransformations(HepRuleCall hepRuleCall) {
    RelNode original = hepRuleCall.rel(0);
    List<RelNode> results = hepRuleCall.getResults();

    results.forEach(
        result -> {
          if (skipRule(hepRuleCall.getRule())) {
            return;
          }

          // Perform some safety checks to guard against no-op transformations:

          // 1)
          // Check for reference equality.
          assert original != result
              : "Attempting to transform to same object reference.\nInput:\n"
                  + RelOptUtil.toString(original, SqlExplainLevel.DIGEST_ATTRIBUTES)
                  + "\nOutput:\n"
                  + RelOptUtil.toString(original, SqlExplainLevel.DIGEST_ATTRIBUTES);

          // 2)
          // Check for object equality. Even if the new node is a different reference, it could
          // still be equivalent in terms of the content.
          assert !MoreRelOptUtil.compareDigests(original, result)
              : "Attempting to transform to equivalent relational expression:\n"
                  + RelOptUtil.toString(original, SqlExplainLevel.DIGEST_ATTRIBUTES);
        });

    return false;
  }

  private static boolean skipRule(RelOptRule rule) {
    for (Class<?> ruleClass : WHITELIST) {
      if (ruleClass.isAssignableFrom(rule.getClass())) {
        return true;
      }
    }
    return false;
  }
}
