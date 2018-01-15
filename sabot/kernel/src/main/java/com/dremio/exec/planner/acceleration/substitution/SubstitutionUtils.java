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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.util.graph.DefaultDirectedGraph;
import org.apache.calcite.util.graph.DefaultEdge;
import org.apache.calcite.util.graph.DirectedGraph;
import org.apache.calcite.util.graph.Graphs;
import org.apache.calcite.util.graph.TopologicalOrderIterator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Utility methods for finding substitutions.
 */
public final class SubstitutionUtils {
  private SubstitutionUtils() { }

  public static Set<RelOptTable> findTables(final RelNode node) {
    final Set<RelOptTable> usedTables = Sets.newLinkedHashSet();
    final RelVisitor visitor = new RelVisitor() {
      @Override public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        if (node instanceof TableScan) {
          usedTables.add(node.getTable());
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
  public static boolean usesTable(final RelOptTable table, final Set<RelOptTable> usedTables,
                                  final Graphs.FrozenGraph<List<String>, DefaultEdge> usesGraph) {
    for (RelOptTable queryTable : usedTables) {
      if (usesGraph.getShortestPath(queryTable.getQualifiedName(),
        table.getQualifiedName()) != null) {
        return true;
      }
    }
    return false;
  }

  public static List<RelOptMaterialization> findApplicableMaterializations(
    final RelNode query, final Collection<RelOptMaterialization> materializations) {
    // Given materializations:
    //   T = Emps Join Depts
    //   T2 = T Group by C1
    // graph will contain
    //   (T, Emps), (T, Depts), (T2, T)
    // and therefore we can deduce T2 uses Emps.
    final DirectedGraph<List<String>, DefaultEdge> usesGraph = DefaultDirectedGraph.create();
    final Map<List<String>, RelOptMaterialization> qnameMap = Maps.newHashMap();
    for (RelOptMaterialization materialization : materializations) {
      // If materialization is a tile in a lattice, we will deal with it shortly.
      if (materialization.table != null && materialization.starTable == null) {
        final List<String> qname = materialization.table.getQualifiedName();
        qnameMap.put(qname, materialization);
        for (RelOptTable usedTable : SubstitutionUtils.findTables(materialization.queryRel)) {
          usesGraph.addVertex(qname);
          usesGraph.addVertex(usedTable.getQualifiedName());
          usesGraph.addEdge(usedTable.getQualifiedName(), qname);
        }
      }
    }

    // Use a materialization if uses at least one of the tables are used by
    // the query. (Simple rule that includes some materializations we won't
    // actually use.)
    final Graphs.FrozenGraph<List<String>, DefaultEdge> frozenGraph =
      Graphs.makeImmutable(usesGraph);

    final Set<RelOptTable> queryTablesUsed = SubstitutionUtils.findTables(query);
    final ImmutableList.Builder<RelOptMaterialization> builder = ImmutableList.builder();

    for (final List<String> qname : TopologicalOrderIterator.of(usesGraph)) {
      RelOptMaterialization materialization = qnameMap.get(qname);
      if (materialization != null && SubstitutionUtils.usesTable(materialization.table,
        queryTablesUsed, frozenGraph)) {
        builder.add(materialization);
      }
    }

    return builder.build();
  }

}


