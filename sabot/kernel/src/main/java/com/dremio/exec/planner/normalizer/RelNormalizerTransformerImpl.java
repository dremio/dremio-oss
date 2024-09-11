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
import com.dremio.exec.ops.ViewExpansionContext;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.events.FunctionDetectedEvent;
import com.dremio.exec.planner.events.PlannerEventBus;
import com.dremio.exec.planner.logical.PreProcessRel;
import com.dremio.exec.planner.logical.RedundantSortEliminator;
import com.dremio.exec.planner.logical.UncollectToFlattenConverter;
import com.dremio.exec.planner.logical.ValuesRewriteShuttle;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.RexCorrelVariableSchemaFixer;
import com.dremio.exec.planner.sql.RexShuttleRelShuttle;
import com.dremio.exec.planner.sql.handlers.PlanLogUtil;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.exec.work.foreman.SqlUnsupportedException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelNormalizerTransformerImpl implements RelNormalizerTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(RelNormalizerTransformerImpl.class);
  private final HepPlannerRunner hepPlannerRunner;
  private final NormalizerRuleSets normalizerRuleSets;
  private final SubstitutionProvider substitutionProvider;
  private final ViewExpansionContext viewExpansionContext;
  private final PlannerSettings plannerSettings;
  private final PlannerCatalog plannerCatalog;
  private final PlannerEventBus plannerEventBus;

  public RelNormalizerTransformerImpl(
      HepPlannerRunner hepPlannerRunner,
      NormalizerRuleSets normalizerRuleSets,
      SubstitutionProvider substitutionProvider,
      ViewExpansionContext viewExpansionContext,
      PlannerSettings plannerSettings,
      PlannerCatalog plannerCatalog,
      PlannerEventBus plannerEventBus) {
    this.hepPlannerRunner = hepPlannerRunner;
    this.normalizerRuleSets = normalizerRuleSets;
    this.substitutionProvider = substitutionProvider;
    this.viewExpansionContext = viewExpansionContext;
    this.plannerSettings = plannerSettings;
    this.plannerCatalog = plannerCatalog;
    this.plannerEventBus = plannerEventBus;
  }

  @Override
  public RelNode transform(RelNode relNode, AttemptObserver attemptObserver)
      throws SqlUnsupportedException {

    try {
      final RelNode normalizedRel = transformPreSerialization(relNode, attemptObserver);

      attemptObserver.planSerializable(normalizedRel);
      updateOriginalRoot(normalizedRel);

      final RelNode expansionNodesRemoved = ExpansionNode.removeFromTree(normalizedRel);
      final RelNode normalizedRel2 = transformPostSerialization(expansionNodesRemoved);

      PlanLogUtil.log("INITIAL", normalizedRel2, LOGGER, null);
      attemptObserver.setNumJoinsInUserQuery(MoreRelOptUtil.countJoins(normalizedRel2));

      return normalizedRel2;
    } finally {
      attemptObserver.tablesCollected(plannerCatalog.getAllRequestedTables());
    }
  }

  @Override
  public RelNode transformForReflection(
      RelNode relNode, RelTransformer relTransformer, AttemptObserver attemptObserver)
      throws SqlUnsupportedException {
    try {
      final RelNode normalizedRel = transformPreSerialization(relNode, attemptObserver);

      final RelNode transformed = relTransformer.transform(normalizedRel);

      attemptObserver.planSerializable(transformed);
      updateOriginalRoot(transformed);

      final RelNode expansionNodesRemoved = ExpansionNode.removeFromTree(transformed);
      final RelNode normalizedRel2 = transformPostSerialization(expansionNodesRemoved);

      PlanLogUtil.log("INITIAL", normalizedRel2, LOGGER, null);
      attemptObserver.setNumJoinsInUserQuery(MoreRelOptUtil.countJoins(normalizedRel2));

      return normalizedRel2;
    } finally {
      attemptObserver.tablesCollected(plannerCatalog.getAllRequestedTables());
    }
  }

  @Override
  public RelNode transformPreSerialization(RelNode relNode, AttemptObserver attemptObserver) {
    // This has to be the very first step, since it will expand RexSubqueries to Correlates and all
    // the other
    // transformations can't operate on the RelNode inside of RexSubquery.
    RelNode expanded =
        hepPlannerRunner.transform(relNode, normalizerRuleSets.createEntityExpansion());
    if (plannerSettings.options.getOption(PlannerSettings.FIX_CORRELATE_VARIABLE_SCHEMA)) {
      expanded = RexCorrelVariableSchemaFixer.fixSchema(expanded);
    }

    RelNode drrsMatched =
        DRRMatcher.match(expanded, substitutionProvider, viewExpansionContext, attemptObserver);
    RelNode valuesRewritten = ValuesRewriteShuttle.rewrite(drrsMatched);
    // We don't have an execution for uncollect, so flatten needs to be part of the normalized
    // query.
    RelNode uncollectsReplaced = UncollectToFlattenConverter.convert(valuesRewritten);
    RelNode aggregateRewritten =
        hepPlannerRunner.transform(uncollectsReplaced, normalizerRuleSets.createAggregateRewrite());
    // We need to do all this detection analysis before the reduce rules fire, since the function
    // might get converted to a literal.
    RelNode expandedButNotReduced = aggregateRewritten;
    dispatchFunctions(expandedButNotReduced, plannerEventBus);
    RelNode reduced = reduce(aggregateRewritten);

    return reduced;
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
    return reduced;
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

  private static void dispatchFunctions(RelNode relNode, PlannerEventBus plannerEventBus) {
    RexShuttleRelShuttle relShuttle =
        new RexShuttleRelShuttle(
            new RexShuttle() {
              @Override
              public RexNode visitCall(RexCall call) {
                SqlOperator sqlOperator = call.getOperator();
                FunctionDetectedEvent functionDetectedEvent =
                    new FunctionDetectedEvent(sqlOperator);
                plannerEventBus.dispatch(functionDetectedEvent);
                return super.visitCall(call);
              }
            });
    relNode.accept(relShuttle);
  }
}
