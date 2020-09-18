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
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.planner.sql.handlers.RelTransformer;
import com.dremio.service.namespace.NamespaceKey;
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
  default SubstitutionStream findSubstitutions(final RelNode query) {
    return SubstitutionStream.empty();
  }

  class SubstitutionStream {
    private final Stream<Substitution> substitutionStream;
    private final Runnable onSuccess;
    private final Consumer<Throwable> onFailure;

    public SubstitutionStream(Stream<Substitution> substitutionStream, Runnable onSuccess, Consumer<Throwable> onFailure) {
      this.substitutionStream = substitutionStream;
      this.onSuccess = onSuccess;
      this.onFailure = onFailure;
    }

    public Stream<Substitution> stream() {
      return substitutionStream;
    }

    public void success() {
      onSuccess.run();
    }

    public void failure(Throwable t) {
      onFailure.accept(t);
    }

    public static SubstitutionStream empty() {
      return new SubstitutionStream(Stream.empty(), () -> { }, t -> { });
    }
  }

  /**
   * Wraps the given RelNode within an ExpansionNode. If any default raw reflection is available for the given RelNode,
   * replace the view with the reflection before wrapping.
   *
   * @param path             Path of the view
   * @param query            RelNode to wrap
   * @param vdsFields        List of all the fields in the VDS
   * @param rowType          Row data type
   * @param contextSensitive If the expansion node is context sensitive
   * @return Wrapped RelNode
   */
  default RelNode wrapExpansionNode(NamespaceKey path, final RelNode query, List<String> vdsFields, RelDataType rowType, boolean contextSensitive) {
    return ExpansionNode.wrap(path, query, rowType, contextSensitive, false);
  }

  default boolean isDefaultRawReflectionEnabled() {
    return false;
  }

  default void disableDefaultRawReflection() {}

  default void resetDefaultRawReflection() {}

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

