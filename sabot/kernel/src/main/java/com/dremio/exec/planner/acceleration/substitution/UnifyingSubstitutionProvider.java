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
package com.dremio.exec.planner.acceleration.substitution;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.MaterializedViewSubstitutionVisitor;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.CoreRules;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.acceleration.DremioMaterialization;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.logical.PushFilterPastProjectRule;
import com.google.common.collect.Lists;

/**
 * A {@link SubstitutionProvider} that employs unification rules to find
 * substitutes.
 */
public class UnifyingSubstitutionProvider extends AbstractSubstitutionProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(UnifyingSubstitutionProvider.class);

  public UnifyingSubstitutionProvider(
    final MaterializationProvider provider) {
    super(provider);
  }

  @Override
  public SubstitutionStream findSubstitutions(final RelNode query) {
    final List<DremioMaterialization> materializations =
      SubstitutionUtils.findApplicableMaterializations(query, getMaterializations());

    final List<Substitution> substitutions = Lists.newArrayList(Substitution.createRootEquivalent(query));
    for (final DremioMaterialization materialization : materializations) {
      final int count = substitutions.size();
      for (int i = 0; i < count; i++) {
        try {
          substitutions.addAll(substitute(substitutions.get(i).getReplacement(), materialization));
        } catch (final Throwable ex) {
          LOGGER.warn("unable to apply materialization: {}", materialization, ex);
        }
      }
    }

    // discard the original query
    return new SubstitutionStream(substitutions.subList(1, substitutions.size()).stream(), () -> { }, t ->  { });
  }

  protected HepProgramBuilder getProgramBuilder() {
    return new HepProgramBuilder()
      .addRuleInstance(PushFilterPastProjectRule.CALCITE_NO_CHILD_CHECK)
      .addRuleInstance(CoreRules.PROJECT_MERGE)
      .addRuleInstance(CoreRules.PROJECT_REMOVE);
  }

  /**
   * Normalizes the given query and target {@link RelNode}s and finds substitutions on canonical
   * representations.
   *
   * @param query  incoming query
   * @param materialization  materialization to apply
   * @return  list of substitutions
   */
  protected List<Substitution> substitute(
      final RelNode query,
      final DremioMaterialization materialization) {

    // Push filters to the bottom, and combine projects on top.
    final HepProgram program = getProgramBuilder().build();
    final HepPlanner hepPlanner = new HepPlanner(program);

    hepPlanner.setRoot(materialization.getQueryRel());
    final RelNode canonicalTarget = hepPlanner.findBestExp();

    hepPlanner.setRoot(query);
    final RelNode canonicalQuery = hepPlanner.findBestExp();

    return substitute(canonicalQuery, canonicalTarget, materialization.getTableRel());
  }

  /**
   * Finds substitutions for the given query and target such that each satisfiable instance of
   * target is replaced with replacement.
   *
   * @param query  incoming query
   * @param target  target to replace
   * @param replacement  replacement that is computationally equivalent to target
   * @return  list of substitutions
   */
  protected List<Substitution> substitute(
    final RelNode query, final RelNode target, final RelNode replacement) {
    return new MaterializedViewSubstitutionVisitor(ExpansionNode.removeFromTree(target), ExpansionNode.removeFromTree(query)).go(replacement).stream().map(Substitution::createRootEquivalent).collect(Collectors.toList());
  }

  public static UnifyingSubstitutionProvider of(
    final MaterializationProvider provider) {
    return new UnifyingSubstitutionProvider(provider);
  }

}
