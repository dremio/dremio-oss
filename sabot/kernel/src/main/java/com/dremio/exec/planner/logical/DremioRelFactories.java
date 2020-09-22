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

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Contains factory implementation for creating various Dremio Logical Rel nodes.
 */

public class DremioRelFactories {

  // Same as {@link RelFactories#LOGICAL_BUILDER}, except DEFAULT_MATCH_FACTORY.
  // This must be used for all rules borrowed from Calcite.
  public static final RelBuilderFactory CALCITE_LOGICAL_BUILDER =
    RelBuilder.proto(
      Contexts.of(
        RelFactories.DEFAULT_PROJECT_FACTORY,
        RelFactories.DEFAULT_FILTER_FACTORY,
        RelFactories.DEFAULT_JOIN_FACTORY,
        RelFactories.DEFAULT_SEMI_JOIN_FACTORY,
        RelFactories.DEFAULT_SORT_FACTORY,
        RelFactories.DEFAULT_AGGREGATE_FACTORY,
     /* RelFactories.DEFAULT_MATCH_FACTORY, */
        RelFactories.DEFAULT_SET_OP_FACTORY,
        RelFactories.DEFAULT_VALUES_FACTORY,
        RelFactories.DEFAULT_TABLE_SCAN_FACTORY));

  public static final RelFactories.ProjectFactory LOGICAL_PROJECT_FACTORY = new ProjectFactoryImpl();
  public static final RelFactories.FilterFactory LOGICAL_FILTER_FACTORY = new FilterFactoryImpl();
  public static final RelFactories.AggregateFactory LOGICAL_AGGREGATE_FACTORY = new AggregateFactoryImpl();
  public static final RelFactories.JoinFactory LOGICAL_JOIN_FACTORY = new JoinFactoryImpl();
  public static final RelFactories.SortFactory LOGICAL_SORT_FACTORY = new SortFactoryImpl();
  public static final RelFactories.CorrelateFactory LOGICAL_CORRELATE_FACTORY = new CorrelateFactoryImpl();
  public static final RelFactories.SetOpFactory LOGICAL_UNION_FACTORY = new SetOpFactoryImpl();

  public static final RelBuilderFactory LOGICAL_BUILDER = RelBuilder.proto(
      Contexts.of(
          LOGICAL_FILTER_FACTORY,
          LOGICAL_JOIN_FACTORY,
          LOGICAL_AGGREGATE_FACTORY,
          LOGICAL_PROJECT_FACTORY,
          LOGICAL_SORT_FACTORY,
          LOGICAL_CORRELATE_FACTORY,
          LOGICAL_UNION_FACTORY));

  public static final RelFactories.ProjectFactory LOGICAL_PROJECT_PROPAGATE_FACTORY = new ProjectPropagateFactoryImpl();
  public static final RelFactories.FilterFactory LOGICAL_FILTER_PROPAGATE_FACTORY = new FilterPropagateFactoryImpl();
  public static final RelFactories.JoinFactory LOGICAL_JOIN_PROPAGATE_FACTORY = new JoinPropagateFactoryImpl();

  public static final RelBuilderFactory LOGICAL_PROPAGATE_BUILDER = RelBuilder.proto(
      Contexts.of(
          LOGICAL_FILTER_PROPAGATE_FACTORY,
          LOGICAL_JOIN_PROPAGATE_FACTORY,
          LOGICAL_PROJECT_PROPAGATE_FACTORY));

  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalProject}.
   */
  private static class ProjectFactoryImpl implements RelFactories.ProjectFactory {
    @Override
    public RelNode createProject(RelNode child,
                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
      final RelOptCluster cluster = child.getCluster();
      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER);
      final RelNode project = ProjectRel.create(cluster, child.getTraitSet().plus(Rel.LOGICAL), child, childExprs, rowType);

      return project;
    }
  }

  /**
   * Implementation of {@link RelFactories.ProjectFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalProject} with child converted.
   */
  private static class ProjectPropagateFactoryImpl implements RelFactories.ProjectFactory {
    @Override
    public RelNode createProject(RelNode child,
                                 List<? extends RexNode> childExprs, List<String> fieldNames) {
      final RelOptCluster cluster = child.getCluster();
      final RelDataType rowType = RexUtil.createStructType(cluster.getTypeFactory(), childExprs, fieldNames, SqlValidatorUtil.F_SUGGESTER);
      final RelNode project = ProjectRel.create(
          cluster,
          child.getTraitSet().plus(Rel.LOGICAL),
          RelOptRule.convert(child, child.getTraitSet().plus(Rel.LOGICAL).simplify()),
          childExprs,
          rowType);

      return project;
    }
  }

  private static class AggregateFactoryImpl implements RelFactories.AggregateFactory {
    @Override
    public RelNode createAggregate(final RelNode child, final boolean indicator, final ImmutableBitSet groupSet, final ImmutableList<ImmutableBitSet> groupSets, final List<AggregateCall> aggCalls) {
      final RelOptCluster cluster = child.getCluster();
      final RelTraitSet traitSet = child.getTraitSet().plus(Rel.LOGICAL);
      try {
        return AggregateRel.create(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
        // Semantic error not possible. Must be a bug. Convert to
        // internal error.
        throw new AssertionError(e);
      }
    }
  }

  /**
   * Implementation of {@link RelFactories.SortFactory} that
   * returns a vanilla {@link SortRel} with offset and fetch.
   */
  private static class SortFactoryImpl implements RelFactories.SortFactory {
    @Override
    public RelNode createSort(RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
      return createSort(input.getTraitSet(), input, collation, offset, fetch);
    }

    @Override
    public RelNode createSort(RelTraitSet traits, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
      RelNode newInput;
      if (!collation.getFieldCollations().isEmpty()) {
        collation = RelCollationTraitDef.INSTANCE.canonize(collation);
        newInput = SortRel.create(input.getCluster(), traits, input, collation, null, null);
        traits = newInput.getTraitSet();
      } else {
        newInput = input;
      }

      if (!isOffsetEmpty(offset) || !isFetchEmpty(fetch)) {
        return LimitRel.create(newInput.getCluster(), traits, newInput, offset, fetch);
      }
      return newInput;
    }
  }

  public static boolean isOffsetEmpty(RexNode offset) {
    return offset == null;
  }

  public static boolean isFetchEmpty(RexNode fetch) {
    return fetch == null;
  }

  /**
   * Implementation of {@link RelFactories.CorrelateFactory} that
   * returns a vanilla {@link CorrelateRel}.
   */
  private static class CorrelateFactoryImpl implements RelFactories.CorrelateFactory {
    @Override
    public RelNode createCorrelate(RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, SemiJoinType joinType) {
      return new CorrelateRel(left.getCluster(), left.getTraitSet(), left, right, correlationId, requiredColumns, joinType);
    }
  }

  /**
   * Implementation of {@link RelFactories.SetOpFactory} that
   * returns a vanilla {@link UnionRel}.
   */
  private static class SetOpFactoryImpl implements RelFactories.SetOpFactory {

    public RelNode createSetOp(SqlKind kind, List<RelNode> inputs, boolean all) {
      final RelOptCluster cluster = inputs.get(0).getCluster();
      final RelTraitSet traitSet = cluster.traitSetOf(Rel.LOGICAL);
      switch(kind) {
        case UNION:
          try {
            return new UnionRel(cluster, traitSet, inputs, all, true);
          } catch (InvalidRelException e) {
            throw new AssertionError(e);
          }
        default:
          throw new AssertionError("Dremio doesn't currently support " + kind + " operations.");
      }
    }
  }

  /**
   * Implementation of {@link RelFactories.FilterFactory} that
   * returns a vanilla {@link FilterRel}.
   */
  private static class FilterFactoryImpl implements RelFactories.FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition, Set<CorrelationId> correlVariables) {
      Preconditions.checkArgument(correlVariables.isEmpty());
      return FilterRel.create(child, condition);
    }
  }


  /**
   * Implementation of {@link RelFactories.FilterFactory} that
   * returns a vanilla {@link FilterRel} with child converted.
   */
  private static class FilterPropagateFactoryImpl implements RelFactories.FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition, Set<CorrelationId> correlVariables) {
      Preconditions.checkArgument(correlVariables.isEmpty());
      return FilterRel.create(
          RelOptRule.convert(child, child.getTraitSet().plus(Rel.LOGICAL).simplify()),
          condition);
    }
  }

  /**
   * Implementation of {@link RelFactories.JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  private static class JoinFactoryImpl implements RelFactories.JoinFactory {
    @Override
    public RelNode createJoin(RelNode left, RelNode right,
                              RexNode condition,
                              Set<CorrelationId> variablesSet,
                              JoinRelType joinType, boolean semiJoinDone) {
      return JoinRel.create(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
    }

    @Override
    public RelNode createJoin(RelNode left, RelNode right,
                              RexNode condition, JoinRelType joinType,
                              Set<String> variablesStopped, boolean semiJoinDone) {
      return JoinRel.create(left.getCluster(), left.getTraitSet(), left, right, condition, joinType);
    }
  }

  /**
   * Implementation of {@link RelFactories.JoinFactory} that returns a vanilla
   * {@link org.apache.calcite.rel.logical.LogicalJoin}.
   */
  private static class JoinPropagateFactoryImpl implements RelFactories.JoinFactory {
    @Override
    public RelNode createJoin(RelNode left, RelNode right,
                              RexNode condition,
                              Set<CorrelationId> variablesSet,
                              JoinRelType joinType, boolean semiJoinDone) {
      return JoinRel.create(
          left.getCluster(),
          left.getTraitSet().plus(Rel.LOGICAL),
          RelOptRule.convert(left, left.getTraitSet().plus(Rel.LOGICAL).simplify()),
          RelOptRule.convert(right, right.getTraitSet().plus(Rel.LOGICAL).simplify()),
          condition,
          joinType);
    }

    @Override
    public RelNode createJoin(RelNode left, RelNode right,
                              RexNode condition, JoinRelType joinType,
                              Set<String> variablesStopped, boolean semiJoinDone) {
      return JoinRel.create(
          left.getCluster(),
          left.getTraitSet().plus(Rel.LOGICAL),
          RelOptRule.convert(left, left.getTraitSet().plus(Rel.LOGICAL).simplify()),
          RelOptRule.convert(right, right.getTraitSet().plus(Rel.LOGICAL).simplify()),
          condition,
          joinType);
    }
  }

}
