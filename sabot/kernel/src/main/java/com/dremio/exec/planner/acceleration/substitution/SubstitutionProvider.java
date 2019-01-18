/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.google.common.base.Preconditions;

/**
 * An interface that suggests substitutions to {@link RelOptPlanner planner}.
 *
 * Given a set of materialized view definitions (Vs) and a query(Q), SubstitutionProvider is in
 * charge of finding R, a subset of Vs such that Q is satisfiable when rewritten in terms of R.
 */
public interface SubstitutionProvider {

  /**
   * Computes and returns a set of possible substitutions for the given query.
   * If the equivalent node for the substition is null, that means the substition should be considered
   * equivalent to the originalRoot
   *
   * @param query  query to rewrite in terms of materialized view definitions.
   * @return  set of substitutions.
   */
  List<Substitution> findSubstitutions(final RelNode query);

  void setPostSubstitutionTransformer(RelTransformer transformer);

  /**
   * A class that represents a substitution. This indicates that the {@link RelNode} replacement is equivalent to equivalent
   * If equivalent is null, treat replacement as equivalent to the originalRoot
   */
  class Substitution {
    private final RelNode replacement;
    private final RelNode equivalent;

    private Substitution(final RelNode replacement) {
      this.replacement = replacement;
      this.equivalent = null;
    }

    public Substitution(final RelNode replacement, final RelNode equivalent) {
      this.replacement = Preconditions.checkNotNull(replacement);
      this.equivalent = Preconditions.checkNotNull(equivalent);
    }

    public RelNode getReplacement() {
      return replacement;
    }

    public boolean considerThisRootEquivalent() {
      return equivalent == null;
    }

    public RelNode getEquivalent() {
      return Preconditions.checkNotNull(equivalent);
    }

    public static Substitution createRootEquivalent(final RelNode replacement) {
      return new Substitution(replacement);
    }
  }
}

