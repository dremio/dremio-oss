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
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;
import org.apache.calcite.util.graph.TopologicalOrderIterator;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.service.Pointer;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Utility methods for finding substitutions.
 */
public final class SubstitutionUtils {
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

}


