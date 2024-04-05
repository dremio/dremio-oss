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
package com.dremio.exec.planner.common;

import com.dremio.exec.planner.physical.PlannerSettings;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

/**
 * Create this to handle all metadata queries in a single clause. Empty, and should not be used to
 * match rules. Only for metadata convenience.
 */
public abstract class SampleRelBase extends SingleRel {

  public static final int MINIMUM_SAMPLE_SIZE = 10;

  /**
   * Creates a <code>SampleRelBase</code>.
   *
   * @param cluster Cluster this relational expression belongs to
   * @param traits Conventions/Distribution/Collation traits
   * @param input Input relational expression
   */
  protected SampleRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
    super(cluster, traits, input);
  }

  public static long getSampleSizeAndSetMinSampleSize(
      PlannerSettings plannerSettings, long denominator) {
    long sampleSize =
        Math.max(SampleRelBase.MINIMUM_SAMPLE_SIZE, plannerSettings.getLeafLimit() / denominator);
    plannerSettings.setMinimumSampleSize(sampleSize);
    return sampleSize;
  }
}
