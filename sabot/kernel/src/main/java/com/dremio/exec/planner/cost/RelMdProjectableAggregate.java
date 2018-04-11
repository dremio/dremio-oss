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
package com.dremio.exec.planner.cost;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.BuiltInMethod;

import com.dremio.exec.planner.logical.ProjectableSqlAggFunctions;
import com.google.common.collect.ImmutableList;

/**
 * a metadata handler for projectable aggregates.
 */
public class RelMdProjectableAggregate implements MetadataHandler<Metadata> {
  private static final RelMdProjectableAggregate INSTANCE =
      new RelMdProjectableAggregate();

  public static final RelMetadataProvider SOURCE =
      ChainedRelMetadataProvider.of(
          ImmutableList.of(
              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltInMethod.ROW_COUNT.method, INSTANCE),

              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltInMethod.MAX_ROW_COUNT.method, INSTANCE),

              ReflectiveRelMetadataProvider.reflectiveSource(
                  BuiltInMethod.NON_CUMULATIVE_COST.method, INSTANCE)));

  // Not used...
  @Override
  public MetadataDef<Metadata> getDef() {
    return null;
  }

  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    if (ProjectableSqlAggFunctions.isProjectableAggregate(rel)) {
      return mq.getRowCount(rel.getInput());
    }

    // try the next handler
    return null;
  }

  public Double getMaxRowCount(Aggregate rel, RelMetadataQuery mq) {
    if (ProjectableSqlAggFunctions.isProjectableAggregate(rel)) {
      return mq.getMaxRowCount(rel);
    }

    // try the next handler
    return null;
  }

  public RelOptCost getNonCumulativeCost(Aggregate aggregate, RelMetadataQuery mq) {
    if (ProjectableSqlAggFunctions.isProjectableAggregate(aggregate)) {
      return aggregate.getCluster().getPlanner().getCostFactory().makeInfiniteCost();
    }

    // try the next handler...
    // Note that Calcite handler is defined in
    // {@code org.apache.calcite.rel.metadata.RelMdPercentageOriginalRows}...
    return null;
  }
}
