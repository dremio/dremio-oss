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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;


/**
 * Base class for logical and physical Aggregations implemented in Dremio
 */
public abstract class AggregateRelBase extends Aggregate {

  public AggregateRelBase(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
      ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
  }

  @Override public double estimateRowCount(RelMetadataQuery mq) {
    // Assume that each sort column has 50% of the value count.
    // Therefore one sort column has .5 * rowCount,
    // 2 sort columns give .75 * rowCount.
    // Zero sort columns yields 1 row (or 0 if the input is empty).
    final int groupCount = groupSet.cardinality();
    if (groupCount == 0) {
      return 1;
    } else {
      double rowCount = super.estimateRowCount(mq);
      rowCount *= 1.0 - Math.pow(.9, groupCount);
      return rowCount;
    }
  }
}
