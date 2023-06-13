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

import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import com.google.common.collect.ImmutableSet;

/**
 * "REGEXP_LIKE is similar to the LIKE condition,
 * except REGEXP_LIKE performs regular expression matching instead of the simple pattern matching performed by LIKE."
 *
 * We noticed that for simple patterns LIKE performs about twice as fast as REGEXP_LIKE.
 * This is most likely due to the fact that LIKE is running less code, since it has a simplier pattern language.
 *
 * Since we don't have access to either implementation
 * we can instead write a rule to use LIKE in place of REGEXP_LIKE in certain scenarios.
 *
 * REGEXP_LIKE has the following special characters +, *, ?, ^, $, (, ), [, ], {, }, |, \
 * which means that none of the other characters have special meaning.
 * If a pattern doesn't have any special characters, then we can safely convert REGEXP_LIKE to LIKE.
 *
 * For example:
 *
 *  SELECT *
 *  FROM EMP
 *  WHERE REGEXP_LIKE(name, 'asdf')
 *
 * Can we rewritten to:
 *
 * SELECT *
 * FROM EMP
 * WHERE LIKE(name, '%asdf%')
 */
public final class RegexpLikeToLikeRule extends RelOptRule {
  public static final RegexpLikeToLikeRule INSTANCE = new RegexpLikeToLikeRule();

  private RegexpLikeToLikeRule() {
    super(operand(RelNode.class, any()), "RegexpLikeToLikeRule");
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    RelNode relNode = relOptRuleCall.rel(0);
    RexBuilder rexBuilder = relNode.getCluster().getRexBuilder();
    InternalRexShuttle shuttle = new InternalRexShuttle(rexBuilder);

    RelNode rewrittenQuery = relNode.accept(shuttle);
    relOptRuleCall.transformTo(rewrittenQuery);
  }

  private static final class InternalRexShuttle extends RexShuttle {
    private static final Set<Character> SPECIAL_CHARACTERS = ImmutableSet.of(
      '.', '+', '*', '?',
      '^','$', '(', ')',
      '[', ']', '{', '}', '|', '\\');

    private final RexBuilder rexBuilder;

    public InternalRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // If the call isn't REGEXP_LIKE, then just recurse on the children calls.
      if (!call.op.getName().equalsIgnoreCase("REGEXP_LIKE")) {
        boolean[] update = new boolean[]{false};
        List<RexNode> clonedOperands = this.visitList(call.operands, update);
        return update[0] ? rexBuilder.makeCall(call.op, clonedOperands) : call;
      }

      // At this point we know it's a REGEXP_LIKE call.
      // If the pattern has special characters, then don't do a rewrite.
      String pattern = ((RexLiteral)call.getOperands().get(1)).getValueAs(String.class);
      for (int i = 0; i < pattern.length(); i++) {
        Character patternCharacter = pattern.charAt(i);
        if (SPECIAL_CHARACTERS.contains(patternCharacter)) {
          return call;
        }
      }

      // The pattern doesn't have special characters,
      // so just convert it to a regular LIKE call.
      RexNode sourceString = call.getOperands().get(0);
      RexNode newPattern = rexBuilder.makeLiteral("%" + pattern + "%");
      return rexBuilder.makeCall(SqlStdOperatorTable.LIKE, sourceString, newPattern);
    }
  }
}
