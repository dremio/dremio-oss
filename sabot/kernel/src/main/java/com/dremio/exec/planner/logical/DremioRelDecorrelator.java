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

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.service.Pointer;

/**
 * Dremio version of RelDecorrelator extended from Calcite
 */
public class DremioRelDecorrelator extends RelDecorrelator {

  private boolean isRelPlanning;

  protected DremioRelDecorrelator(
      CorelMap cm,
      Context context,
      RelBuilder relBuilder,
      boolean forceValueGenerator,
      boolean isRelPlanning) {
    super(cm, context, relBuilder, forceValueGenerator);
    this.isRelPlanning = isRelPlanning;
  }

  @Override
  protected RelBuilderFactory relBuilderFactory() {
    if (isRelPlanning) {
      return DremioRelFactories.LOGICAL_BUILDER;
    }
    return DremioRelFactories.CALCITE_LOGICAL_BUILDER;
  }

  /** Decorrelates a query.
   *
   * <p>This is the main entry point to {@code DremioRelDecorrelator}.
   *
   * @param rootRel Root node of the query
   * @param forceValueGenerator force value generator to be created when decorrelating filters
   * @param relBuilder        Builder for relational expressions
   *
   * @return Equivalent query with all
   * {@link Correlate} instances removed
   */
  public static RelNode decorrelateQuery(RelNode rootRel,
    RelBuilder relBuilder, boolean forceValueGenerator, boolean isRelPlanning) {
    final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
    if (!corelMap.hasCorrelation()) {
      return rootRel;
    }
    final RelOptCluster cluster = rootRel.getCluster();
    final DremioRelDecorrelator decorrelator =
      new DremioRelDecorrelator(corelMap, cluster.getPlanner().getContext(), relBuilder, forceValueGenerator, isRelPlanning);
    RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);
    if (!decorrelator.cm.getMapCorToCorRel().isEmpty()) {
      newRootRel = decorrelator.decorrelate(newRootRel);
    }
    return newRootRel;
  }

  public static RelNode decorrelateQuery(RelNode rootRel, RelBuilder relBuilder, boolean isRelPlanning) {
    final RelNode decorrelatedWithValueGenerator = decorrelateQuery(rootRel, relBuilder, true, isRelPlanning);
    if (correlateCount(decorrelatedWithValueGenerator) != 0) {
      return decorrelateQuery(rootRel, relBuilder, false, isRelPlanning);
    }
    return decorrelatedWithValueGenerator;
  }

  public static int correlateCount(RelNode rel) {
    final Pointer<Integer> count = new Pointer<>(0);
    rel.accept(new StatelessRelShuttleImpl() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof Correlate) {
          count.value++;
        }
        return super.visit(other);
      }

      @Override
      public RelNode visit(LogicalCorrelate correlate) {
        count.value++;
        return super.visit(correlate);
      }
    });
    return count.value;
  }
}
