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

package com.dremio.exec.planner.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import com.dremio.exec.planner.common.WindowRelBase;

public class WindowRel extends WindowRelBase implements Rel {
  /**
   * Creates a window relational expression.
   *
   * @param cluster Cluster
   * @param traits
   * @param child   Input relational expression
   * @param rowType Output row type
   * @param groups Windows
   */
  private WindowRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    super(cluster, traits, child, constants, rowType, groups);
  }

  public static WindowRel create(RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      List<RexLiteral> constants,
      RelDataType rowType,
      List<Group> groups) {
    RelTraitSet traits = adjustTraits(cluster, child, groups, traitSet);
    return new WindowRel(cluster, traits, child, constants, rowType, groups);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new WindowRel(getCluster(), traitSet, sole(inputs), constants, getRowType(), groups);
  }
}


