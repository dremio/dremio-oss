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

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilderFactory;

/**
 * For a query like:
 *
 * <p>SELECT * FROM people WHERE FOO(name) IN ('son', 'john', 'bob')
 *
 * <p>It can get translated to:
 *
 * <p>SELECT * FROM people WHERE (FOO(name) = 'son') OR (FOO(name) = 'john') OR (FOO(name) = 'bob')
 *
 * <p>Notice that FOO(name) gets evaluated 3 times and FOO(x) might be an expensive expression to
 * evaluate.
 *
 * <p>We can apply common subexpression elimination (CSE) to mitigate this by rewriting the query
 * like so:
 *
 * <p>let temp = FOO(name); SELECT * FROM people WHERE temp IN ('son', 'john', 'bob')
 *
 * <p>Basically store the result of the common subexpression in a temp variable (or projection rel
 * node) and reference that.
 */
public final class InClauseCommonSubexpressionEliminationRule extends RelOptRule {
  public static final InClauseCommonSubexpressionEliminationRule INSTANCE =
      new InClauseCommonSubexpressionEliminationRule(DremioRelFactories.LOGICAL_BUILDER);

  private InClauseCommonSubexpressionEliminationRule(RelBuilderFactory factory) {
    super(operand(FilterRel.class, any()), factory, "InClauseCommonSubexpressionEliminationRule");
  }

  @Override
  public boolean matches(RelOptRuleCall relOptRuleCall) {
    final FilterRel filterRel = relOptRuleCall.rel(0);
    return RelOptUtil.conjunctions(filterRel.getCondition()).stream()
        .map(rexNode -> rexNode.getKind())
        .filter(sqlKind -> sqlKind == SqlKind.OR)
        .findAny()
        .isPresent();
  }

  @Override
  public void onMatch(RelOptRuleCall relOptRuleCall) {
    final FilterRel filterRel = relOptRuleCall.rel(0);
    final RexCall condition = (RexCall) filterRel.getCondition();

    Set<RexNodeWithDeepEqualsAndHash> complexCommonSubexpressions =
        getCommonComplexSubexpressions(condition);
    if (complexCommonSubexpressions.isEmpty()) {
      return;
    }

    final List<RexNode> projectNodes =
        MoreRelOptUtil.identityProjects(filterRel.getInput().getRowType());
    final Map<RexNodeWithDeepEqualsAndHash, Integer> commonSubExpressionToProjectionIndex =
        new HashMap<>();
    for (RexNodeWithDeepEqualsAndHash commonSubexpression : complexCommonSubexpressions) {
      int index = projectNodes.size();
      projectNodes.add(commonSubexpression.getRexNode());
      commonSubExpressionToProjectionIndex.put(commonSubexpression, index);
    }

    RexNode rewrittenCondition =
        getRewrittenFilter(filterRel, commonSubExpressionToProjectionIndex);

    List<RexNode> originalColumns =
        MoreRelOptUtil.identityProjects(filterRel.getInput().getRowType());

    final RelNode rewrittenRelNode =
        relOptRuleCall
            .builder()
            .push(filterRel.getInput())
            // Eliminate the complex common sub-expressions.
            // We do this by pushing the common sub-expression to the projection and referencing
            // that.
            .project(projectNodes)
            .filter(rewrittenCondition)
            // Drop the extra columns we introduced earlier
            .project(originalColumns)
            .build();

    relOptRuleCall.transformTo(rewrittenRelNode);
  }

  private static Optional<RexNode> tryGetComplexChildExpression(RexNode node) {
    // Check two see if we have an expression in the form "x = a" or "a = x"
    // where 'x' is a complex expression
    // and 'a' is any expression.
    if (!(node instanceof RexCall)) {
      return Optional.empty();
    }

    RexCall rexCall = (RexCall) node;
    if (rexCall.op.kind != SqlKind.EQUALS) {
      return Optional.empty();
    }

    RexNode lhs = rexCall.operands.get(0);
    if (lhs instanceof RexCall) {
      return Optional.of(lhs);
    }

    RexNode rhs = rexCall.operands.get(1);
    if (rhs instanceof RexCall) {
      return Optional.of(rhs);
    }

    return Optional.empty();
  }

  private static Set<RexNodeWithDeepEqualsAndHash> getCommonComplexSubexpressions(
      RexCall condition) {
    // Find all the duplicate complex common sub-expressions.
    // Only check for the following type of pattern:
    // (x = a) OR (x = b) OR (x = c) ... (x = n)
    // where x is non trivial expression.
    // We don't attempt to look for nested sub expressions
    final ImmutableList<RexNode> operands = condition.operands;
    Set<RexNodeWithDeepEqualsAndHash> seenRexNodes = new HashSet<>();
    Set<RexNodeWithDeepEqualsAndHash> duplicateRexNodes = new HashSet<>();
    for (RexNode operand : operands) {
      Optional<RexNode> optionalComplexChildExpression = tryGetComplexChildExpression(operand);
      if (!optionalComplexChildExpression.isPresent()) {
        continue;
      }

      RexNode complexChildExpression = optionalComplexChildExpression.get();

      RexNodeWithDeepEqualsAndHash rexNodeWithDeepEqualsAndHash =
          new RexNodeWithDeepEqualsAndHash(complexChildExpression);
      if (!seenRexNodes.add(rexNodeWithDeepEqualsAndHash)) {
        duplicateRexNodes.add(rexNodeWithDeepEqualsAndHash);
      }
    }

    return duplicateRexNodes;
  }

  private static RexNode getRewrittenFilter(
      FilterRel filterRel,
      Map<RexNodeWithDeepEqualsAndHash, Integer> commonSubexpressionToProjectionIndex) {
    // At this point all the common subexpressions are pushed to the projection
    // We need to rewrite the filter to reference these common subexpression from the projection.
    final RexCall condition = (RexCall) filterRel.getCondition();
    final ImmutableList<RexNode> operands = condition.operands;
    final RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();

    final List<RexNode> rewrittenOperands = new ArrayList<>();

    for (RexNode operand : operands) {
      Optional<RexNode> optionalComplexChildExpression = tryGetComplexChildExpression(operand);
      RexNode rewrittenOperand;
      if (!optionalComplexChildExpression.isPresent()) {
        rewrittenOperand = operand;
      } else {
        RexNode complexChildExpression = optionalComplexChildExpression.get();

        RexNodeWithDeepEqualsAndHash rexNodeWithDeepEqualsAndHash =
            new RexNodeWithDeepEqualsAndHash(complexChildExpression);
        Integer projectionIndex =
            commonSubexpressionToProjectionIndex.get(rexNodeWithDeepEqualsAndHash);
        if (projectionIndex == null) {
          rewrittenOperand = operand;
        } else {
          RexCall binaryExpression = (RexCall) operand;
          RexNode binaryOperand0 = binaryExpression.operands.get(0);
          RexNode binaryOperand1 = binaryExpression.operands.get(1);

          RexNode nonComplexSubexpression =
              complexChildExpression == binaryOperand0 ? binaryOperand1 : binaryOperand0;

          RexInputRef rexInputRef =
              new RexInputRef(projectionIndex, complexChildExpression.getType());
          List<RexNode> rewrittenBinaryOperands = new ArrayList<>();
          rewrittenBinaryOperands.add(rexInputRef);
          rewrittenBinaryOperands.add(nonComplexSubexpression);

          rewrittenOperand =
              rexBuilder.makeCall(
                  binaryExpression.type, binaryExpression.op, rewrittenBinaryOperands);
        }
      }

      rewrittenOperands.add(rewrittenOperand);
    }

    RexNode rewrittenCondition =
        rexBuilder.makeCall(condition.type, condition.op, rewrittenOperands);

    return rewrittenCondition;
  }

  private static final class RexNodeWithDeepEqualsAndHash {
    private final RexNode rexNode;

    public RexNodeWithDeepEqualsAndHash(RexNode rexNode) {
      assert rexNode != null;
      this.rexNode = rexNode;
    }

    public RexNode getRexNode() {
      return this.rexNode;
    }

    @Override
    public int hashCode() {
      return this.rexNode.toString().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }

      if (obj == null) {
        return false;
      }

      if (!(obj instanceof RexNodeWithDeepEqualsAndHash)) {
        return false;
      }

      final RexNodeWithDeepEqualsAndHash other = (RexNodeWithDeepEqualsAndHash) obj;
      return (this.rexNode == other.rexNode)
          || (this.rexNode.toString().equals(other.rexNode.toString()));
    }
  }
}
