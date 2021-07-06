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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;

import com.google.common.collect.Sets;

/**
 * Rewrite filter conditions to avoid NLJ
 * E.g., (t1.x = t2.y AND ….) OR (t1.x = t2.y AND ….)
 * can be rewritten as (t1.x = t2.y) AND (… OR …)
 */
public abstract class ExtractCommonConjunctionRule extends RelOptRule {

  protected ExtractCommonConjunctionRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static class RexConjunctionExtractor extends RexShuttle {
    private final RexBuilder rexBuilder;
    public RexConjunctionExtractor(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }
    @Override
    public RexNode visitCall(final RexCall call) {
      RexNode newNode = super.visitCall(call);
      switch (newNode.getKind()) {
        case AND:
          return RexUtil.flatten(rexBuilder, newNode);
        case OR:
          List<RexNode> disjunctions = RelOptUtil.disjunctions(newNode);
          Set<RexNodeWrapper> exprs = null;
          // for each element input in the OR clause, get a set of conjunctions, and then find the intersection
          // of the conjunctions across all inputs
          for (RexNode rex : disjunctions) {
            Set<RexNodeWrapper> finalExprs = new HashSet<>();
            RelOptUtil.conjunctions(rex).forEach(c -> finalExprs.add(RexNodeWrapper.of(c)));
            if (exprs == null) {
              exprs = finalExprs;
            } else {
              exprs = Sets.intersection(exprs, finalExprs);
            }
            if (exprs.isEmpty()) {
              return newNode;
            }
          }

          Set<RexNodeWrapper> finalExprs1 = exprs;
          List<RexNode> newDisjunctions = new ArrayList<>();
          // now that we have the common conjunctions, they can be removed from the inputs
          for (RexNode rex : disjunctions) {
            RexNode newRex = RexUtil.composeConjunction(rexBuilder,
              RelOptUtil.conjunctions(rex).stream()
                .filter(r -> !finalExprs1.contains(RexNodeWrapper.of(r)))
                .collect(Collectors.toList()), false);
            newDisjunctions.add(newRex);
          }
          RexNode newDisJunction = RexUtil.composeDisjunction(rexBuilder, newDisjunctions, false);
          List<RexNode> newConjunctions = new ArrayList<>();
          // add the extracted conjunctions to the list
          exprs.forEach(e -> newConjunctions.add(e.rexNode));
          // add the remaining expressions in the original disjunction
          newConjunctions.add(newDisJunction);
          return RexUtil.composeConjunction(rexBuilder, newConjunctions, false);
        default:
          return newNode;
      }
    }

  }

  private static class RexNodeWrapper {
    private RexNode rexNode;
    private RexNodeWrapper(RexNode rexNode) {
      this.rexNode = rexNode;
    }
    static RexNodeWrapper of(RexNode rexNode) {
      return new RexNodeWrapper(rexNode);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RexNodeWrapper that = (RexNodeWrapper) o;
      return rexNode.toString().equals(that.rexNode.toString());
    }

    @Override
    public int hashCode() {
      return rexNode.toString().hashCode();
    }
  }

}
