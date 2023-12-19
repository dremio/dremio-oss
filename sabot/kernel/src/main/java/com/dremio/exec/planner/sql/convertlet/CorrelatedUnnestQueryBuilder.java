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
package com.dremio.exec.planner.sql.convertlet;

import java.util.Collections;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

public final class CorrelatedUnnestQueryBuilder {
  private final RexCorrelVariable rexCorrelVariable;
  private final RelBuilder relBuilder;
  private final RexBuilder rexBuilder;

  public CorrelatedUnnestQueryBuilder(
    RexCorrelVariable rexCorrelVariable,
    RelBuilder relBuilder,
    RexBuilder rexBuilder) {
    this.rexCorrelVariable = rexCorrelVariable;
    this.relBuilder = relBuilder;
    this.rexBuilder = rexBuilder;
  }

  public static CorrelatedUnnestQueryBuilder create(ConvertletContext context) {
    return new CorrelatedUnnestQueryBuilder(
      context.getRexCorrelVariable(),
      context.getRelBuilder(),
      context.getRexBuilder());
  }

  /**
   * UNNESTS the array expression and handles correlation.
   * @param arrayExpression
   * @return The next builder state.
   */
  public AfterCorrelatedUnnest unnest(RexNode arrayExpression) {
    return unnest(arrayExpression, rexCorrelVariable);
  }

  public AfterCorrelatedUnnest unnest(RexNode arrayExpression, RexCorrelVariable rexCorrelVariable) {
    // The outer RelNode will define the source.
    relBuilder.values(new String[]{"ZERO"}, 0);

    // Replace InputRefs with Correlated Variable
    RexNode rewrittenArrayExpression = RexInputRefToFieldAccess.rewrite(
      arrayExpression,
      rexBuilder,
      rexCorrelVariable);

    // We only want the correlationId in the outermost RexSubQuery
    Pair<CorrelationId, RexNode> removeResult = CorrelationIdRemover.remove(rewrittenArrayExpression);
    rewrittenArrayExpression = removeResult.right;

    // If a rewrite happen, then we need to introduce a CorrelationID
    boolean correlationIdNeeded = arrayExpression != rewrittenArrayExpression;
    CorrelationId correlationId = correlationIdNeeded ? rexCorrelVariable.id : null;

    RelNode unnest = relBuilder
      .project(rewrittenArrayExpression)
      .uncollect(Collections.EMPTY_LIST, false)
      .build();

    return new AfterCorrelatedUnnest(relBuilder,  unnest, correlationId);
  }

  private static final class RexInputRefToFieldAccess extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final RexCorrelVariable rexCorrelVariable;

    private RexInputRefToFieldAccess(RexBuilder rexBuilder, RexCorrelVariable rexCorrelVariable) {
      this.rexBuilder = rexBuilder;
      this.rexCorrelVariable = rexCorrelVariable;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      RexNode correlatedFieldAccess = rexBuilder.makeFieldAccess(rexCorrelVariable, inputRef.getIndex());
      return correlatedFieldAccess;
    }

    public static RexNode rewrite(
      RexNode node,
      RexBuilder rexBuilder,
      RexCorrelVariable rexCorrelVariable) {
      RexInputRefToFieldAccess visitor = new RexInputRefToFieldAccess(rexBuilder, rexCorrelVariable);
      return node.accept(visitor);
    }
  }
}
