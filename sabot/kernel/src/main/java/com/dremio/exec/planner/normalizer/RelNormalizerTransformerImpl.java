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

import com.dremio.exec.ops.PlannerCatalog;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.events.NonIncrementalRefreshFunctionEvent;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.RedundantSortEliminator;
import com.dremio.exec.planner.logical.UncollectToFlattenConverter;
import com.dremio.exec.planner.logical.ValuesRewriteShuttle;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.NonCacheableFunctionDetector;
import com.dremio.exec.planner.sql.handlers.EmptyRelPropagator;
import com.dremio.exec.planner.sql.handlers.PlanLogUtil;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import org.apache.calcite.rel.RelNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelNormalizerTransformerImpl implements RelNormalizerTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelNormalizerTransformerImpl.class);
  private final HepPlannerRunner hepPlannerRunner;
  private final NormalizerRuleSets normalizerRuleSets;
  private final PlannerSettings plannerSettings;
  private final PlannerCatalog plannerCatalog;

  public RelNormalizerTransformerImpl(
      HepPlannerRunner hepPlannerRunner,
      NormalizerRuleSets normalizerRuleSets,
      PlannerSettings plannerSettings,
      PlannerCatalog plannerCatalog) {
    this.hepPlannerRunner = hepPlannerRunner;
    this.normalizerRuleSets = normalizerRuleSets;
    this.plannerSettings = plannerSettings;
    this.plannerCatalog = plannerCatalog;
  }

  @Override
  public PreSerializedQuery transform(RelNode relNode, AttemptObserver attemptObserver)
      throws SqlUnsupportedException {

    try {
      final PreSerializedQuery normalizedRel = transformPreSerialization(relNode);

      attemptObserver.planSerializable(normalizedRel.getPlan());
      updateOriginalRoot(normalizedRel.getPlan());

      final RelNode expansionNodesRemoved = ExpansionNode.removeFromTree(normalizedRel.getPlan());
      final RelNode normalizedRel2 = transformPostSerialization(expansionNodesRemoved);

      PlanLogUtil.log("INITIAL", normalizedRel2, LOGGER, null);
      attemptObserver.setNumJoinsInUserQuery(MoreRelOptUtil.countJoins(normalizedRel2));

      return normalizedRel.withNewPlan(normalizedRel2);
    } finally {
      attemptObserver.tablesCollected(plannerCatalog.getAllRequestedTables());
    }
  }

  @Override
  public PreSerializedQuery transformForCompactAndMaterializations(
      RelNode relNode,
      RelTransformer relTransformer,
      AttemptObserver attemptObserver,
      PlannerEventBus plannerEventBus)
      throws SqlUnsupportedException {
    try {
      final PreSerializedQuery normalizedRel = transformPreSerialization(relNode);

      if (!normalizedRel
          .getNonCacheableFunctionDetectorResults()
          .isReflectionIncrementalRefreshable()) {
        plannerEventBus.dispatch(NonIncrementalRefreshFunctionEvent.INSTANCE);
      }

      final RelNode transformed = relTransformer.transform(normalizedRel.getPlan());

      attemptObserver.planSerializable(transformed);
      updateOriginalRoot(transformed);

      final RelNode expansionNodesRemoved = ExpansionNode.removeFromTree(transformed);
      final RelNode normalizedRel2 = transformPostSerialization(expansionNodesRemoved);

      PlanLogUtil.log("INITIAL", normalizedRel2, LOGGER, null);
      attemptObserver.setNumJoinsInUserQuery(MoreRelOptUtil.countJoins(normalizedRel2));

      return normalizedRel.withNewPlan(normalizedRel2);
    } finally {
      attemptObserver.tablesCollected(plannerCatalog.getAllRequestedTables());
    }
  }

  @Override
  public PreSerializedQuery transformPreSerialization(RelNode relNode) {
    // This has to be the very first step, since it will expand RexSubqueries to Correlates and all
    // the other
    // transformations can't operate on the RelNode inside of RexSubquery.
    RelNode expanded =
        hepPlannerRunner.transform(relNode, normalizerRuleSets.createEntityExpansion());
    RelNode valuesRewritten = ValuesRewriteShuttle.rewrite(expanded);
    // We don't have an execution for uncollect, so flatten needs to be part of the normalized
    // query.
    RelNode uncollectsReplaced = UncollectToFlattenConverter.convert(valuesRewritten);
    RelNode aggregateRewritten =
        hepPlannerRunner.transform(uncollectsReplaced, normalizerRuleSets.createAggregateRewrite());
    NonCacheableFunctionDetector.Result result =
        NonCacheableFunctionDetector.detect(aggregateRewritten);
    RelNode reduced = reduce(aggregateRewritten);

    return new PreSerializedQuery(reduced, result);
  }

  @Override
  public RelNode transformPostSerialization(RelNode relNode) throws SqlUnsupportedException {
    final RelNode expansionNodesRemoved = ExpansionNode.removeFromTree(relNode);
    RelNode expandedOperators =
        hepPlannerRunner.transform(
            expansionNodesRemoved, normalizerRuleSets.createOperatorExpansion());
    RelNode values = ValuesRewriteShuttle.rewrite(expandedOperators);
    RelNode preprocessedRel = preprocess(values);

    return preprocessedRel;
  }

  private RelNode reduce(RelNode relNode) {
    RelNode reduced =
        hepPlannerRunner.transform(relNode, normalizerRuleSets.createReduceExpression());
    reduced =
        plannerSettings.isSortInJoinRemoverEnabled()
            ? RedundantSortEliminator.apply(reduced)
            : reduced;
    return EmptyRelPropagator.propagateEmptyRel(reduced);
  }

  private RelNode preprocess(RelNode relNode) throws SqlUnsupportedException {
    PreProcessRel visitor = PreProcessRel.createVisitor(relNode.getCluster().getRexBuilder());
    try {
      return relNode.accept(visitor);
    } catch (UnsupportedOperationException ex) {
      visitor.convertException();
      throw ex;
    }
  }

  private static void updateOriginalRoot(RelNode relNode) {
    final DremioVolcanoPlanner volcanoPlanner =
        (DremioVolcanoPlanner) relNode.getCluster().getPlanner();
    volcanoPlanner.setOriginalRoot(relNode);
  }
}
