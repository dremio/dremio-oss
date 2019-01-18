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
package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.common.logical.data.LogicalOperator;
import com.dremio.exec.planner.common.JdbcRelBase;

public class JdbcRel extends JdbcRelBase implements Rel, CopyToCluster {

  public JdbcRel(RelOptCluster cluster, RelTraitSet traits, RelNode jdbcSubTree) {
    super(cluster, traits, jdbcSubTree);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> ignored /* inputs */) {
    return new JdbcRel(getCluster(), traitSet, jdbcSubTree);
  }

  @Override
  public LogicalOperator implement(LogicalPlanImplementor implementor) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    final RelNode copiedSubTree = getSubTree().accept(copier);
    return new JdbcRel(
      copier.getCluster(),
      getTraitSet(),
      copiedSubTree
    );
  }
}
