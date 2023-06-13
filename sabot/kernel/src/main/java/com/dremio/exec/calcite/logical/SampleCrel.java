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
package com.dremio.exec.calcite.logical;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;

import com.dremio.exec.planner.common.SampleRelBase;
import com.dremio.exec.planner.physical.PlannerSettings;

/**
 * Calcite logical rel for sampling. Current sampling is just applying a leaf level limit.
 */
public final class SampleCrel extends SampleRelBase implements CopyToCluster {

  public static final int MINIMUM_SAMPLE_SIZE = 10;

  public static long getSampleSizeAndSetMinSampleSize(PlannerSettings plannerSettings, long denominator) {
    long sampleSize = Math.max(SampleCrel.MINIMUM_SAMPLE_SIZE, plannerSettings.getLeafLimit() / denominator);
    plannerSettings.setMinimumSampleSize(sampleSize);
    return sampleSize;
  }

  public SampleCrel(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
    super(cluster, traitSet, input);
  }

  public static SampleCrel create(RelNode input) {
    final RelCollation collation = RelCollationTraitDef.INSTANCE.canonize(RelCollations.EMPTY);
    final RelTraitSet newTraitSet = input.getTraitSet().replace(Convention.NONE).replace(collation);
    return new SampleCrel(input.getCluster(), newTraitSet, input);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SampleCrel(this.getCluster(), traitSet, sole(inputs));
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    final RelNode input = getInput().accept(copier);
    return new SampleCrel(
      copier.getCluster(),
      getTraitSet(),
      input
    );
  }
}
