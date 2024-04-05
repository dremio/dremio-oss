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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

/**
 * Visitor to shorten field aliases in the JDBC subtree to match what the target dialect can
 * support. Also optionally adds a LogicalProject node to map the user-requested aliases to the
 * shortened names.
 */
public class ShortenJdbcColumnAliases extends StatelessRelShuttleImpl {

  public static final SqlValidatorUtil.Suggester SHORT_ALIAS_SUGGESTER =
      (original, attempt, size) -> "$SA" + attempt;

  private Set<String> usedAliases = Sets.newHashSet();

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof JdbcCrel) {
      final JdbcCrel logical = (JdbcCrel) other;

      // Eliminate subsets to enforce that all nodes in the Jdbc subtree are JdbcRelImpl nodes.
      final RelNode subsetRemoved = logical.getInput().accept(new MoreRelOptUtil.SubsetRemover());
      logical.replaceInput(0, subsetRemoved);

      if (logical.getPluginId() == null) {
        return logical;
      }

      final RelNode updatedJdbcRoot = super.visitChildren(logical);

      // Add a LogicalProject to map the original set of column aliases if necessary.
      if (logical.getRowType().equals(updatedJdbcRoot.getRowType())) {
        return updatedJdbcRoot;
      }

      final LogicalProject logicalProject =
          LogicalProject.create(
              updatedJdbcRoot,
              ImmutableList.of(),
              updatedJdbcRoot
                  .getCluster()
                  .getRexBuilder()
                  .identityProjects(updatedJdbcRoot.getRowType()),
              other.getRowType());

      return logicalProject;

    } else if (other instanceof JdbcRelImpl) {
      final RelNode updatedNode = super.visitChildren(other);

      final JdbcRelImpl jdbcNode = (JdbcRelImpl) updatedNode;
      return jdbcNode.shortenAliases(SHORT_ALIAS_SUGGESTER, usedAliases);
    }
    return super.visit(other);
  }
}
