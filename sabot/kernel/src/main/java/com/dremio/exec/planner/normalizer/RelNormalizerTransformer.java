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
package com.dremio.exec.planner.normalizer;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.RedundantSortEliminator;
import com.dremio.exec.planner.logical.UncollectToFlattenConverter;
import com.dremio.exec.planner.logical.ValuesRewriteShuttle;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.work.foreman.SqlUnsupportedException;

public class RelNormalizerTransformer {
  private final HepPlannerRunner hepPlannerRunner;
  private final NormalizerRuleSets normalizerRuleSets;
  private final PlannerSettings plannerSettings;

  public RelNormalizerTransformer(
    HepPlannerRunner hepPlannerRunner,
    NormalizerRuleSets normalizerRuleSets,
    PlannerSettings plannerSettings) {
    this.hepPlannerRunner = hepPlannerRunner;
    this.normalizerRuleSets = normalizerRuleSets;
    this.plannerSettings = plannerSettings;
  }

  public RelNode transformPreSerialization(RelNode relNode) {
    // This has to be the very first step, since it will expand RexSubqueries to Correlates and all the other
    // transformations can't operate on the RelNode inside of RexSubquery.
    RelNode expanded = hepPlannerRunner.transform(relNode, normalizerRuleSets.createEntityExpansion());
    // We don't have an execution for uncollect, so flatten needs to be part of the normalized query.
    RelNode uncollectsReplaced = UncollectToFlattenConverter.convert(expanded);
    RelNode aggregateRewritten = hepPlannerRunner.transform(uncollectsReplaced, normalizerRuleSets.createAggregateRewrite());
    RelNode reduced = reduce(aggregateRewritten);
    return reduced;
  }

  public RelNode transformPostSerialization(RelNode relNode) throws SqlUnsupportedException {
    RelNode expandedOperators = hepPlannerRunner.transform(relNode, normalizerRuleSets.createOperatorExpansion());
    RelNode values = expandedOperators.accept(new ValuesRewriteShuttle());
    RelNode preprocessedRel = preprocess(values);

    return preprocessedRel;
  }

  private RelNode reduce(RelNode relNode) {
    RelNode reduced = hepPlannerRunner.transform(relNode, normalizerRuleSets.createReduceExpression());
    return plannerSettings.isSortInJoinRemoverEnabled()
      ?  RedundantSortEliminator.apply(reduced)
      : reduced;
  }

  private RelNode preprocess(RelNode relNode) throws SqlUnsupportedException {
    PreProcessRel visitor = PreProcessRel.createVisitor(
      relNode.getCluster().getRexBuilder());
    try {
      return relNode.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }
  }
}
