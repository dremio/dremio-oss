/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.MaterializedViewSubstitutionVisitor;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
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

  @Override public List<Substitution> findSubstitutions(final RelNode query) {
    final List<RelOptMaterialization> materializations =
      SubstitutionUtils.findApplicableMaterializations(query, getMaterializations());

    final List<Substitution> substitutions = Lists.newArrayList(new Substitution(query, null));
    for (final RelOptMaterialization materialization : materializations) {
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
    return substitutions.subList(1, substitutions.size());
  }

  @Override
  public RelNode processPostPlanning(RelNode rel) {
    return rel;
  }

  protected HepProgramBuilder getProgramBuilder() {
    return new HepProgramBuilder()
      .addRuleInstance(FilterProjectTransposeRule.INSTANCE)
      .addRuleInstance(ProjectMergeRule.INSTANCE)
      .addRuleInstance(ProjectRemoveRule.INSTANCE);
  }

  /**
   * Normalizes the given query and target {@link RelNode}s and finds substitutions on canonical
   * representations.
   *
   * @param query  incoming query
   * @param materialization  materialization to apply
   * @return  list of substitutions
   */
  protected List<Substitution> substitute(RelNode query,
                                                   final RelOptMaterialization materialization) {
    // First, if the materialization is in terms of a star table, rewrite
    // the query in terms of the star table.
    if (materialization.starTable != null) {
      final RelNode newRoot = RelOptMaterialization.tryUseStar(query,
        materialization.starRelOptTable);
      if (newRoot != null) {
        query = newRoot;
      }
    }

    // Push filters to the bottom, and combine projects on top.
    final HepProgram program = getProgramBuilder().build();
    final HepPlanner hepPlanner = new HepPlanner(program);

    hepPlanner.setRoot(materialization.queryRel);
    final RelNode canonicalTarget = hepPlanner.findBestExp();

    hepPlanner.setRoot(query);
    final RelNode canonicalQuery = hepPlanner.findBestExp();

    return substitute(canonicalQuery, canonicalTarget, materialization.tableRel);
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
    return Lists.transform(new MaterializedViewSubstitutionVisitor(target, query).go(replacement), new Function<RelNode, Substitution>() {
      @Override
      public Substitution apply(RelNode input) {
        return new Substitution(input, null);
      }
    });
  }

  public static UnifyingSubstitutionProvider of(
    final MaterializationProvider provider) {
    return new UnifyingSubstitutionProvider(provider);
  }

}
