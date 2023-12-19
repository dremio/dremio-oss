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
package com.dremio.exec.planner.logical.partition;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Evaluator;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Projections;

import com.dremio.exec.store.iceberg.IcebergExpGenVisitor;
import com.google.common.collect.ImmutableList;

class IcebergNativeEvaluator extends SargPrunableEvaluator {
  private final Evaluator evaluator;
  private final boolean hasConditions;

  public IcebergNativeEvaluator(final PartitionStatsBasedPruner pruner,final  Evaluator evaluator,final  boolean hasConditions) {
    super(pruner);
    this.evaluator = evaluator;
    this.hasConditions = hasConditions;
  }

  @Override
  public boolean hasConditions() {
    return hasConditions;
  }

  @Override
  public boolean isRecordMatch(final StructLike partitionData) {
    if(evaluator != null) {
      return evaluator.eval(partitionData);
    }
    return true;
  }

  public static IcebergNativeEvaluator buildSargPrunableConditions(final PartitionStatsBasedPruner partitionStatsBasedPruner,
                                                                   final ImmutableList<RexCall> rexConditions,
                                                                   final RelDataType rowType,
                                                                   final RelOptCluster cluster,
                                                                   final FindSimpleFilters rexVisitor) {
    final IcebergExpGenVisitor icebergExpGenVisitor = new IcebergExpGenVisitor(rowType, cluster);
    final List<UnprocessedCondition> unprocessedConditions = buildUnprocessedConditions(rexConditions, rexVisitor);
    Expression pruneAndExpression = null;
    for (final UnprocessedCondition unprocessedCondition : unprocessedConditions) {
      final Expression icebergPartitionPruneExpression =
        icebergExpGenVisitor.convertToIcebergExpression(unprocessedCondition.getCondition());
      pruneAndExpression = (pruneAndExpression == null) ? icebergPartitionPruneExpression :
        Expressions.and(pruneAndExpression, icebergPartitionPruneExpression);
    }
    Evaluator evaluator = null;
    if(pruneAndExpression != null) {
      final Expression projected = Projections.inclusive(partitionStatsBasedPruner.getPartitionSpec(), false).project(pruneAndExpression);
      evaluator = new Evaluator(partitionStatsBasedPruner.getPartitionSpec().partitionType(), projected, false);
    }
    return new IcebergNativeEvaluator(partitionStatsBasedPruner, evaluator,pruneAndExpression != null);
  }
}
