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

import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.tools.RelBuilder;

import com.dremio.exec.planner.sql.handlers.RexSubQueryUtils;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;

/**
 * Rewrites a join condition with a sub-query to a Correlate with filter on the right side.
 *
 * Filter conditions transformations:
 * <UL>
 *   <LI>{@link RexInputRef} referring to the left side are rewritten to a {@link RexFieldAccess} with a new {@link CorrelationId} referring to the left side</LI>
 *   <LI>{@link RexInputRef} referring to the right side need to be shifted</LI>
 *   <LI>{@link RexFieldAccess} inside {@link  RexSubQuery} referring to the left side are rewritten with a new {@link CorrelationId} referring to the left side</LI>
 * </UL>
 *
 *
 * The following will be rewritten as:
 * <code>
 *   JOIN condition=subquery(.., cor0)
 *     ....
 *     ....
 * </code>
 *
 * <code>
 *   CORRELATE id=cor1
 *     ...
 *     FILTER filter=subquery(.., cor0)
 * </code>
 *
 * The upstream rule does not handle left joins.
 * @see org.apache.calcite.rel.rules.CoreRules#JOIN_SUB_QUERY_TO_CORRELATE
 */
public class DremioJoinSubQueryRemoveRule extends RelRule<DremioJoinSubQueryRemoveRule.Config> {

  /**
   * Creates a RelRule.
   *
   * @param config
   */
  protected DremioJoinSubQueryRemoveRule(Config config) {
    super(config);
  }

  @Override public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    RelNode left = join.getLeft();
    RelNode right = join.getRight();
    LazyCorrelateId leftCorrelateId = new LazyCorrelateId(join.getCluster());
    RelBuilder builder = call.builder();
    RexBuilder rexBuilder = builder.getRexBuilder();

    RexNode newCondition = rewrite(join, leftCorrelateId);

    call.transformTo(builder
      .push(left)
      .push(right)
      .filter(newCondition)
      .join(join.getJoinType(),
        rexBuilder.makeLiteral(true),
        leftCorrelateId.setOfIds())
      .build());
  }

  /**
   *
   * @param join
   * @param correlationId
   * @return
   */
  private static RexNode rewrite(Join join, LazyCorrelateId correlationId) {
    RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    RelNode left = join.getLeft();
    RelNode right = join.getRight();

    Supplier<RexNode> leftBaseNode =
      Suppliers.memoize(() -> rexBuilder.makeCorrel(left.getRowType(), correlationId.getCorrelationId()));


    int leftSize = left.getRowType().getFieldCount();

    return join.getCondition().accept(new RexShuttle() {
      @Override public RexNode visitInputRef(RexInputRef inputRef) {
        if (inputRef.getIndex() < leftSize) { // is left
          return rexBuilder.makeFieldAccess(leftBaseNode.get(), inputRef.getIndex());
        } else {
          return rexBuilder.makeInputRef(inputRef.getType(), inputRef.getIndex() - leftSize);
        }
      }

      @Override
      public RexNode visitSubQuery(RexSubQuery subQuery) {
        RexNode rightBase = null == subQuery.correlationId
          ? null
          : rexBuilder.makeCorrel(right.getRowType(), subQuery.correlationId);
        RexSubQuery rewrittenSubQuery = (RexSubQuery) super.visitSubQuery(subQuery);
        RewriteSubQueryValues rexShuttle = new RewriteSubQueryValues(
          rexBuilder,
          rewrittenSubQuery.correlationId,
          leftBaseNode,
          rightBase,
          left.getRowType());
        return rewrittenSubQuery.accept(rexShuttle);
      }
    });
  }

  public interface Config extends RelRule.Config{
    Config DEFAULT = RelRule.Config.EMPTY
      .withOperandSupplier(os ->
        os.operand(Join.class)
          .predicate(join -> RexSubQueryUtils.containsSubQuery(join)
            && !join.getJoinType().generatesNullsOnLeft())
          .anyInputs())
      .as(Config.class);

    @Override default RelOptRule toRule() {
      return new DremioJoinSubQueryRemoveRule(this);
    }
  }


  private static class RewriteSubQueryValues extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final CorrelationId correlationId;
    private final Supplier<RexNode> leftCorrelationBase;
    @Nullable private final RexNode rightCorrelateBase;
    private final RelDataType leftRowType;

    final RelShuttle relShuttle;

    public RewriteSubQueryValues(
        RexBuilder rexBuilder,
        CorrelationId correlationId,
        Supplier<RexNode> leftCorrelationBase,
        @Nullable RexNode rightCorrelateBase,
        RelDataType leftRowType) {
      this.rexBuilder = rexBuilder;
      this.correlationId = correlationId;
      this.leftCorrelationBase = leftCorrelationBase;
      this.rightCorrelateBase = rightCorrelateBase;
      this.leftRowType = leftRowType;

      this.relShuttle = new RelShuttleRewriter(this);
    }

    @Override
    public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
      RexNode rexCorrelVariable = fieldAccess.getReferenceExpr();
      if (rexCorrelVariable instanceof RexCorrelVariable
        && ((RexCorrelVariable)rexCorrelVariable).id == correlationId) {
        int fieldIndex = fieldAccess.getField().getIndex();
        if(fieldAccess.getField().getIndex() < leftRowType.getFieldCount()) { // is left
          return rexBuilder.makeFieldAccess(leftCorrelationBase.get(), fieldIndex);
        } else { //is right
          return rexBuilder.makeFieldAccess(rightCorrelateBase, fieldIndex - leftRowType.getFieldCount());
        }
      } else {
        return super.visitFieldAccess(fieldAccess);
      }

    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      return subQuery.clone(subQuery.rel.accept(relShuttle));
    }

  }

  private static class RelShuttleRewriter extends RelHomogeneousShuttle {
    public RelShuttleRewriter(RexShuttle rexShuttle) {
      this.rexShuttle = rexShuttle;
    }

    private final RexShuttle rexShuttle;

    @Override
    public RelNode visit(RelNode other) {
      return super.visit(other)
        .accept(rexShuttle);
    }
  }

  private static class LazyCorrelateId {
    private final RelOptCluster relOptCluster;
    private CorrelationId correlationId = null;

    public LazyCorrelateId(RelOptCluster relOptCluster) {
      this.relOptCluster = relOptCluster;
    }

    public CorrelationId getCorrelationId() {
      if (null == correlationId) {
        correlationId = relOptCluster.createCorrel();
      }
      return correlationId;
    }

    public CorrelationId getRawCorrelationId() {
      return correlationId;
    }

    public Set<CorrelationId> setOfIds() {
      if(null == correlationId) {
        return ImmutableSet.of();
      } else {
        return ImmutableSet.of(correlationId);
      }
    }
  }


}
