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

import com.dremio.exec.planner.common.FlattenRelBase;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexInputRef;

/** FlattenCrel in Dremio's LOGICAL convention. */
public class FlattenRel extends FlattenRelBase implements Rel {

  public FlattenRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexInputRef> toFlatten,
      int numProjectsPushed) {
    this(cluster, traits, child, toFlatten, null, numProjectsPushed);
  }

  public FlattenRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      List<RexInputRef> toFlatten,
      List<String> aliases,
      int numProjectsPushed) {
    super(cluster, traits, child, toFlatten, aliases, numProjectsPushed);
    assert getConvention() == LOGICAL;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FlattenRel(
        getCluster(), traitSet, sole(inputs), toFlatten, aliases, numProjectsPushed);
  }

  @Override
  public FlattenRelBase copy(List<RelNode> inputs, List<RexInputRef> toFlatten) {
    return new FlattenRel(
        getCluster(), getTraitSet(), sole(inputs), toFlatten, aliases, numProjectsPushed);
  }

  public static FlattenRel create(RelNode input, int indexToFlatten, String alias) {
    RexInputRef toFlattenRef =
        input.getCluster().getRexBuilder().makeInputRef(input, indexToFlatten);
    return new FlattenRel(
        input.getCluster(),
        input.getTraitSet(),
        input,
        ImmutableList.of(toFlattenRef),
        ImmutableList.of(alias),
        0);
  }
}
