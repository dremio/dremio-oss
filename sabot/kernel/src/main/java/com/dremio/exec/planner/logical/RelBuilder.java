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

import com.google.common.collect.ImmutableSet;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.Litmus;

public class RelBuilder extends org.apache.calcite.tools.RelBuilder {

  private final RelFactories.Struct struct;

  /**
   * Creates a {@link RelBuilderFactory}, a partially-created RelBuilder. Just add a {@link
   * RelOptCluster} and a {@link RelOptSchema}
   */
  public static RelBuilderFactory proto(final Context context) {
    return new RelBuilderFactory() {
      @Override
      public RelBuilder create(RelOptCluster cluster, RelOptSchema schema) {
        return new RelBuilder(context, cluster, schema);
      }
    };
  }

  public static org.apache.calcite.tools.RelBuilder newCalciteRelBuilderWithoutContext(
      RelOptCluster cluster) {
    return proto((RelNode.Context) null).create(cluster, null);
  }

  protected RelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
    super(context, cluster, relOptSchema);
    if (context == null) {
      context = Contexts.EMPTY_CONTEXT;
    }
    this.struct = Objects.requireNonNull(RelFactories.Struct.fromContext(context));
  }

  @Override
  public RelBuilder join(JoinRelType joinType, RexNode condition, Set<CorrelationId> variablesSet) {
    RelNode right = this.peek();
    ImmutableSet.Builder<CorrelationId> variablesSetBuilder = ImmutableSet.builder();
    for (CorrelationId correlationId : variablesSet) {
      if (!RelOptUtil.notContainsCorrelation(right, correlationId, Litmus.IGNORE)) {
        variablesSetBuilder.add(correlationId);
      }
    }
    super.join(joinType, condition, variablesSetBuilder.build());
    return this;
  }
}
