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
package com.dremio.exec.planner.sql.handlers;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.calcite.logical.JdbcCrel;
import com.dremio.exec.planner.StatelessRelShuttleImpl;
import com.dremio.exec.planner.logical.JdbcRel;
import com.dremio.exec.planner.logical.Rel;

class ConvertJdbcLogicalToJdbcRel extends StatelessRelShuttleImpl {

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof JdbcCrel) {
      JdbcCrel logical = (JdbcCrel) other;
      // Eliminate subsets to enforce that all nodes in the Jdbc subtree are JdbcRelImpl nodes.
      // TODO: When we implement more of the StoragePlugin2 design, it should be easier to just get the StoragePluginId.
      final RelNode subsetRemoved = logical.getInput().accept(new MoreRelOptUtil.SubsetRemover());
      logical.replaceInput(0, subsetRemoved);
      return new JdbcRel(logical.getCluster(), logical.getTraitSet().replace(Rel.LOGICAL), logical.getInput());
    }
    return super.visit(other);
  }
}
