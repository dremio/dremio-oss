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

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.sql.SqlExplainLevel;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.reflection.rules.ReplacementPointer;
import com.dremio.service.Pointer;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Sets;

/**
 * Utility methods for finding substitutions.
 */
public final class SubstitutionUtils {

  private static final RelShuttle REMOVE_REPLACEMENT_POINTER = new StatelessRelShuttleImpl() {
    @Override
    public RelNode visit(RelNode other) {
      if (other instanceof ReplacementPointer) {
        return ((ReplacementPointer) other).getSubTree();
      }
      return super.visit(other);
    }
  };

  private SubstitutionUtils() { }

  public static Set<List<String>> findTables(final RelNode node) {
    final Set<List<String>> usedTables = Sets.newLinkedHashSet();
    final RelVisitor visitor = new RelVisitor() {
      @Override public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        if (node instanceof TableScan) {
          usedTables.add(node.getTable().getQualifiedName());
        }
        super.visit(node, ordinal, parent);
      }
    };

    visitor.go(node);
    return usedTables;
  }

  /**
   * Returns whether {@code table} uses one or more of the tables in
   * {@code usedTables}.
   */
  public static boolean usesTable(final Set<List<String>> tables, final RelNode rel) {
    final Pointer<Boolean> used = new Pointer<>(false);
    rel.accept(new RoutingShuttle() {
      @Override
      public RelNode visit(TableScan scan) {
        if (tables.contains(scan.getTable().getQualifiedName())) {
          used.value = true;
        }
        return scan;
      }
      @Override
      public RelNode visit(RelNode other) {
        if (used.value) {
          return other;
        }
        return super.visit(other);
      }
    });
    return used.value;
  }

  public static List<RelOptMaterialization> findApplicableMaterializations(
    final RelNode query, final Collection<RelOptMaterialization> materializations) {
    final Set<List<String>> queryTablesUsed = SubstitutionUtils.findTables(query);
    return FluentIterable.from(materializations)
      .filter(new Predicate<RelOptMaterialization>() {
        @Override
        public boolean apply(RelOptMaterialization materialization) {
          return usesTable(queryTablesUsed, materialization.queryRel);
        }
      })
      .toList();
  }

  /**
   * @return true if query plan matches the candidate plan, after removing the {@link ReplacementPointer} from the candidate plan
   */
  public static boolean arePlansEqualIgnoringReplacementPointer(RelNode query, RelNode candidate) {
    Preconditions.checkNotNull(query, "query plan required");
    Preconditions.checkNotNull(candidate, "candidate plan required");

    final String queryString = RelOptUtil.toString(query, SqlExplainLevel.DIGEST_ATTRIBUTES);
    final RelNode updatedCandidate = candidate.accept(REMOVE_REPLACEMENT_POINTER);
    final String candidateString = RelOptUtil.toString(updatedCandidate, SqlExplainLevel.DIGEST_ATTRIBUTES);

    return queryString.equals(candidateString);
  }

}


