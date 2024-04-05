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
package com.dremio.exec.planner.cost;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

/** a metadata handler for projectable aggregates. */
public class RelMdCost implements MetadataHandler<Metadata> {
  private static final RelMdCost INSTANCE = new RelMdCost();

  public static final RelMetadataProvider SOURCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(
              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltInMethod.CUMULATIVE_COST.method, INSTANCE),
              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE)));

  // Not used...
  @Override
  public MetadataDef<Metadata> getDef() {
    return null;
  }

  public RelOptCost getCumulativeCost(RelSubset subset, RelMetadataQuery mq) {
    final RelNode best = subset.getBest();
    if (best != null) {
      return mq.getCumulativeCost(best);
    }

    return null;
  }

  public RelOptCost getNonCumulativeCost(RelSubset subset, RelMetadataQuery mq) {
    final RelNode best = subset.getBest();
    if (best != null) {
      return mq.getNonCumulativeCost(best);
    }

    return null;
  }
}
