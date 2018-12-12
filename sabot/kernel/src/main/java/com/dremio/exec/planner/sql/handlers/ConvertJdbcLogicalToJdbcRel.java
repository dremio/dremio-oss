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
package com.dremio.exec.planner.sql.handlers;

import org.apache.calcite.rel.RelNode;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.common.JdbcRelImpl;
import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.JdbcRel;

class ConvertJdbcLogicalToJdbcRel extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof JdbcCrel) {
      final JdbcCrel logical = (JdbcCrel) other;

      // Eliminate subsets to enforce that all nodes in the Jdbc subtree are JdbcRelImpl nodes.
      final RelNode subsetRemoved = logical.getInput().accept(new MoreRelOptUtil.SubsetRemover());
      logical.replaceInput(0, subsetRemoved);

      if (logical.getPluginId() == null) {
        // Reverts JdbcRelImpl nodes back to their original Logical rel. This is necessary to handle subtrees that
        // contain a values operator. We try to pushdown these subtrees if they are joined with another subtree that
        // is pushed down to a jdbc source. After the HEP planner is finished, if we see that there are isolated
        // branches that were not joined, we don't want to push them down, so we must revert.
        return ((JdbcRelImpl) logical.getInput()).revert();
      }

      return new JdbcRel(logical.getCluster(), logical.getTraitSet(), logical.getInput());
    }
    return super.visit(other);
  }
}
