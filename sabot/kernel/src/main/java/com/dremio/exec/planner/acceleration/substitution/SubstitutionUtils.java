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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;

import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.DremioMaterialization;
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

  public static int hash(RelNode rel) {
    Hasher hasher = new Hasher();
    PrintWriter pw = new PrintWriter(hasher, false);
    rel.explain(new RelWriterImpl(pw, SqlExplainLevel.DIGEST_ATTRIBUTES, false));
    return hasher.hash;
  }

  private static class Hasher extends Writer {
    private int hash = 0;

    @Override
    public void write(char[] cbuf, int off, int len) throws IOException {
      int h = hash;
      for (char aCbuf : cbuf) {
        h = 31 * h + aCbuf;
      }
      hash = h;
    }

    @Override
    public void flush() throws IOException { }

    @Override
    public void close() throws IOException { }
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

  public static List<DremioMaterialization> findApplicableMaterializations(
    final RelNode query, final Collection<DremioMaterialization> materializations) {
    final Set<List<String>> queryTablesUsed = SubstitutionUtils.findTables(query);
    return FluentIterable.from(materializations)
      .filter(new Predicate<DremioMaterialization>() {
        @Override
        public boolean apply(DremioMaterialization materialization) {
          return usesTable(queryTablesUsed, materialization.getQueryRel());
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

    final int queryCode = hash(query);
    final RelNode updatedCandidate = candidate.accept(REMOVE_REPLACEMENT_POINTER);
    final int candidateCode = hash(updatedCandidate);

    return queryCode == candidateCode;
  }

}


