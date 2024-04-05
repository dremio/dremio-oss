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

import com.dremio.exec.planner.common.SampleRelBase;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

/** Dremio logical for {@link SampleRelBase} */
public class SampleRel extends SampleRelBase implements Rel {

  public SampleRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child) {
    super(cluster, traitSet, child);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SampleRel(getCluster(), traitSet, sole(inputs));
  }

  public static SampleRel create(RelNode input) {
    return new SampleRel(input.getCluster(), input.getTraitSet().plus(Rel.LOGICAL), input);
  }
}
