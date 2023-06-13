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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.Pair;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.catalog.TableVersionContext;
import com.dremio.exec.planner.RoutingShuttle;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.acceleration.ExpansionNode;
import com.dremio.exec.tablefunctions.ExternalQueryScanCrel;
import com.dremio.reflection.rules.ReplacementPointer;
import com.dremio.service.Pointer;
import com.google.common.base.Preconditions;
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

  public static Set<VersionedPath> findExpansionNodes(final RelNode node) {
    final Set<VersionedPath> usedVdsPaths = new LinkedHashSet<>();
    final RelVisitor visitor = new RelVisitor() {
      @Override
      public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        if (node instanceof ExpansionNode) {
          ExpansionNode expansionNode = (ExpansionNode) node;
          usedVdsPaths.add(VersionedPath.of(expansionNode.getPath().getPathComponents(), expansionNode.getVersionContext()));
        }
        super.visit(node, ordinal, parent);
      }
    };
    visitor.go(node);
    return usedVdsPaths;
  }

  public static boolean isSubstitutableScan(RelNode node) {
    return node instanceof TableScan && !(node instanceof ScanCrel && !((ScanCrel) node).isSubstitutable());
  }

  public static Set<VersionedPath> findTables(final RelNode node) {
    final Set<VersionedPath> usedTables = Sets.newLinkedHashSet();
    final RelVisitor visitor = new RelVisitor() {
      @Override public void visit(final RelNode node, final int ordinal, final RelNode parent) {
        if (isSubstitutableScan(node)) {
          TableVersionContext versionContext = null;
          if (node instanceof ScanCrel) {
            versionContext = ((ScanCrel)node).getTableMetadata().getVersionContext();
          }
          usedTables.add(VersionedPath.of(node.getTable().getQualifiedName(), versionContext));
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
  public static boolean usesTableOrVds(final Set<VersionedPath> tables, final Set<VersionedPath> vdsPaths, final Set<ExternalQueryDescriptor> externalQueries, final RelNode rel) {
    final Pointer<Boolean> used = new Pointer<>(false);
    rel.accept(new RoutingShuttle() {
      @Override
      public RelNode visit(TableScan scan) {
        TableVersionContext versionContext = null;
        if (scan instanceof ScanCrel) {
          versionContext = ((ScanCrel)scan).getTableMetadata().getVersionContext();
        }
        if (tables.contains(VersionedPath.of(scan.getTable().getQualifiedName(), versionContext))) {
          used.value = true;
        }
        return scan;
      }
      @Override
      public RelNode visit(RelNode other) {
        if (used.value) {
          return other;
        }
        if (other instanceof ExternalQueryScanCrel) {
          ExternalQueryScanCrel eq = (ExternalQueryScanCrel) other;
          if (externalQueries.contains(descriptor(eq))) {
            used.value = true;
            return other;
          }
        }
        if (other instanceof ExpansionNode) {
          ExpansionNode expansionNode = (ExpansionNode) other;
          if (vdsPaths.contains(VersionedPath.of(expansionNode.getPath().getPathComponents(), expansionNode.getVersionContext()))) {
            used.value = true;
            return other;
          }
        }
        return super.visit(other);
      }
    });
    return used.value;
  }

  public static ExternalQueryDescriptor descriptor(ExternalQueryScanCrel eq) {
    return new ExternalQueryDescriptor(eq.getPluginId().getName(), eq.getSql());
  }

  public static class ExternalQueryDescriptor {
    private final String source;
    private final String query;

    private ExternalQueryDescriptor(String source, String query) {
      this.source = source;
      this.query = query;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ExternalQueryDescriptor that = (ExternalQueryDescriptor) o;
      return source.equals(that.source) &&
        query.equals(that.query);
    }

    @Override
    public int hashCode() {
      return Objects.hash(source, query);
    }
  }

  public static Set<ExternalQueryDescriptor> findExternalQueries(RelNode query) {
    Set<ExternalQueryDescriptor> externalQueries = new HashSet<>();
    query.accept(new RoutingShuttle() {
      @Override
      public RelNode visit(RelNode other) {
        if (other instanceof ExternalQueryScanCrel) {
          ExternalQueryScanCrel eq = (ExternalQueryScanCrel) other;
          externalQueries.add(descriptor(eq));
        }
        return super.visit(other);
      }
    });
    return externalQueries;
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

  /**
   * VersionedPath is a table/view path with an optional TableVersionContext.
   * For example, an Arctic table could have a "schema"."table" path with a "BRANCH main" table version context.
   * Non-versioned tables such as RDBMS or filesystem parquet will have a null TableVersionContext.
   *
   * Since VersionedPath extends {@link Pair}, we can conveniently use VersionedPath as keys with various Java collections.
   */
  public static final class VersionedPath extends Pair<List<String>, TableVersionContext> {
    /**
     * Creates a Pair.
     *
     * @param path  left value
     * @param versionContext right value
     */
    private VersionedPath(List<String> path, TableVersionContext versionContext) {
      super(path, versionContext);
    }
    public static VersionedPath of(List<String> path, TableVersionContext versionContext) {
      return new VersionedPath(path, versionContext);
    }
    public static VersionedPath of(List<String> path) {
      return new VersionedPath(path, null);
    }
  }

  public static TableVersionContext getVersionContext(RelOptTable table) {
    DremioTable dremioTable = Preconditions.checkNotNull(table.unwrap(DremioTable.class));
    return dremioTable.getDataset().getVersionContext();
  }
}
