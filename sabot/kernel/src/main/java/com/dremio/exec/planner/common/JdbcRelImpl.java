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
package com.dremio.exec.planner.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import com.dremio.exec.catalog.StoragePluginId;
import com.google.common.base.Preconditions;

/**
 * Relational expression to push down to jdbc source
 */
public interface JdbcRelImpl extends RelNode, CopyToCluster {

  /**
   * Get the plugin ID from the JDBC node. A null plugin ID implies that the operation is agnostic to which
   * JDBC data source it is executing on. (Currently only VALUES nodes).
   *
   * @return The plugin ID, or null if the operation is data source agnostic.
   */
  StoragePluginId getPluginId();

  /**
   * Return the logical {@link RelNode} version of this node. Typically, implementations override
   * {@link #revert(List)}, rather than this method directly.
   *
   * @return reverted version of this node as a logical {@link RelNode}
   */
  default RelNode revert() {
    final List<RelNode> inputs = getInputs();

    final List<RelNode> revertedInputs = new ArrayList<>();
    for (int i = 0; i < inputs.size(); i++) {
      final RelNode input = inputs.get(i);
      Preconditions.checkState(input instanceof JdbcRelImpl,
          String.format("%s (#%d) is not a JdbcRelImpl", input.getClass().getSimpleName(), i));
      revertedInputs.add(((JdbcRelImpl) input).revert());
    }

    return revert(revertedInputs);
  }

  /**
   * Return the logical {@link RelNode} version of this node, assuming that the given inputs are converted to logical
   * {@link RelNode}. Typically, consumers of this interface use {@link #revert()}.
   *
   * @param revertedInputs inputs as logical {@link RelNode RelNodes}
   * @return reverted version of this node as a logical {@link RelNode}
   */
  RelNode revert(List<RelNode> revertedInputs);

  /**
   * Return an equivalent node with column aliases that are compatible with the target data source.
   *
   * @param suggester The strategy used to make generated aliases unique.
   * @param usedAliases The set of aliases already used.
   */
  default RelNode shortenAliases(SqlValidatorUtil.Suggester suggester, Set<String> usedAliases) {
    // Default behavior for nodes that do not result in column aliases.
    return this;
  }
}
