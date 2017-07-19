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
package com.dremio.exec.calcite.logical;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;
import com.dremio.exec.planner.common.FlattenRelBase;

/**
 * Rel that represents flatten operator in Calcite's Convention.NONE
 */
public class FlattenCrel extends FlattenRelBase implements CopyToCluster {
  public FlattenCrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, List<RexInputRef> toFlatten, int numProjectsPushed) {
    super(cluster, traits, child, toFlatten, numProjectsPushed);
    assert this.getConvention() == Convention.NONE;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FlattenCrel(getCluster(), traitSet, sole(inputs), toFlatten, numProjectsPushed);
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    final RelNode input = getInput().accept(copier);
    return new FlattenCrel(
      copier.getCluster(),
      getTraitSet(),
      input,
      (List<RexInputRef>) (Object) copier.copyRexNodes(getChildExps()),
      getNumProjectsPushed()
    );
  }
}
