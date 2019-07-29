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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.config.SelectionVectorRemover;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.options.Options;
import com.dremio.options.TypeValidators.LongValidator;
import com.dremio.options.TypeValidators.PositiveLongValidator;

@Options
public class SelectionVectorRemoverPrel extends SinglePrel {

  public static final LongValidator RESERVE = new PositiveLongValidator("planner.op.svremover.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final LongValidator LIMIT = new PositiveLongValidator("planner.op.svremover.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  public SelectionVectorRemoverPrel(Prel child){
    super(child.getCluster(), child.getTraitSet(), child);
  }

  public SelectionVectorRemoverPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
    super(cluster, traits, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SelectionVectorRemoverPrel(this.getCluster(), traitSet, inputs.get(0));
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final PhysicalOperator childPOP = ((Prel)getInput()).getPhysicalOperator(creator);

    return new SelectionVectorRemover(
        creator.props(this, null, childPOP.getProps().getSchema().clone(SelectionVectorMode.NONE), RESERVE, LIMIT),
        childPOP
        );
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

}
