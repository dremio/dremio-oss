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

import com.dremio.exec.planner.common.SortRelBase;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

/** Sort implemented in Dremio. */
public class SortRel extends SortRelBase implements Rel {
  /** Creates a SortRel with offset and fetch. */
  private SortRel(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    super(cluster, traits, input, collation, offset, fetch);
  }

  @Override
  public SortRel copy(
      RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
    return SortRel.create(getCluster(), traitSet, input, collation, offset, fetch);
  }

  public static SortRel create(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, RelCollation collation) {
    return create(cluster, traits, input, collation, null, null);
  }

  public static SortRel create(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode input,
      RelCollation collation,
      RexNode offset,
      RexNode fetch) {
    final RelTraitSet adjustedTraits = adjustTraits(traits, collation);
    return new SortRel(cluster, adjustedTraits, input, collation, offset, fetch);
  }
}
