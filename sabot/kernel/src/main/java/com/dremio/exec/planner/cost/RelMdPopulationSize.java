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

import static com.dremio.exec.planner.cost.RelMdDistinctRowCount.getDistinctRowCountFromEstimateRowCount;
import static com.dremio.exec.planner.cost.RelMdDistinctRowCount.getDistinctRowCountFromTableMetadata;

import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PopulationSize;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableBitSet.Builder;
import org.apache.calcite.util.Util;

import com.dremio.exec.store.sys.statistics.StatisticsService;

/**
 * RelMdPopulationSize supplies a default implementation of
 * {@link RelMetadataQuery#getPopulationSize} for the standard logical algebra.
 */
public class RelMdPopulationSize
  implements MetadataHandler<PopulationSize> {
  private static final RelMdPopulationSize INSTANCE = new RelMdPopulationSize(StatisticsService.NO_OP);
  public static final RelMetadataProvider SOURCE =
    ReflectiveRelMetadataProvider.reflectiveSource(
      BuiltInMethod.POPULATION_SIZE.method, INSTANCE);

  private final StatisticsService statisticsService;
  private boolean isNoOp;

  public RelMdPopulationSize(StatisticsService statisticsService) {
    this.statisticsService = statisticsService;
    this.isNoOp = statisticsService == StatisticsService.NO_OP;
  }

  @Override
  public MetadataDef<PopulationSize> getDef() {
    return PopulationSize.DEF;
  }

  public Double getPopulationSize(RelSubset subset, RelMetadataQuery mq, ImmutableBitSet groupKey) {
    return mq.getPopulationSize(Util.first(subset.getBest(), subset.getOriginal()), groupKey);
  }

  public Double getPopulationSize(TableScan rel, RelMetadataQuery mq, ImmutableBitSet groupKey) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      RexNode cond = rel.getCluster().getRexBuilder().makeLiteral(true);
      return Optional.ofNullable(getDistinctRowCountFromTableMetadata(rel, mq, groupKey, cond, statisticsService))
        .orElse(getDistinctRowCountFromEstimateRowCount(rel, mq, groupKey, cond));
    }
    return null;
  }

  public Double getPopulationSize(Filter rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Sort rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Exchange rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    return mq.getPopulationSize(rel.getInput(), groupKey);
  }

  public Double getPopulationSize(Union rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    Double population = 0.0;
    for (RelNode input : rel.getInputs()) {
      Double subPop = mq.getPopulationSize(input, groupKey);
      if (subPop == null) {
        return null;
      }
      population += subPop;
    }
    return population;
  }

  public Double getPopulationSize(Join rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    return RelMdUtil.getJoinPopulationSize(mq, rel, groupKey);
  }

  public Double getPopulationSize(Aggregate rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    Builder childKey = ImmutableBitSet.builder();
    RelMdUtil.setAggChildKeys(groupKey, rel, childKey);
    return mq.getPopulationSize(rel.getInput(), childKey.build());
  }

  public Double getPopulationSize(Values rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    // assume half the rows are duplicates
    return rel.estimateRowCount(mq) / 2;
  }

  public Double getPopulationSize(Project rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    Builder baseCols = ImmutableBitSet.builder();
    Builder projCols = ImmutableBitSet.builder();
    List<RexNode> projExprs = rel.getProjects();
    RelMdUtil.splitCols(projExprs, groupKey, baseCols, projCols);

    Double population =
      mq.getPopulationSize(rel.getInput(), baseCols.build());
    if (population == null) {
      return null;
    }

    // No further computation required if the projection expressions are
    // all column references
    if (projCols.cardinality() == 0) {
      return population;
    }

    for (int bit : projCols.build()) {
      Double subRowCount =
        RelMdUtil.cardOfProjExpr(mq, rel, projExprs.get(bit));
      if (subRowCount == null) {
        return null;
      }
      population *= subRowCount;
    }

    // REVIEW zfong 6/22/06 - Broadbase did not have the call to
    // numDistinctVals.  This is needed; otherwise, population can be
    // larger than the number of rows in the RelNode.
    return RelMdUtil.numDistinctVals(population, mq.getRowCount(rel));
  }

  /** Catch-all implementation for
   * {@link PopulationSize#getPopulationSize(ImmutableBitSet)},
   * invoked using reflection.
   *
   * @see RelMetadataQuery#getPopulationSize(RelNode, ImmutableBitSet)
   */
  public Double getPopulationSize(RelNode rel, RelMetadataQuery mq,
                                  ImmutableBitSet groupKey) {
    // if the keys are unique, return the row count; otherwise, we have
    // no further information on which to return any legitimate value

    // REVIEW zfong 4/11/06 - Broadbase code returns the product of each
    // unique key, which would result in the population being larger
    // than the total rows in the relnode
    boolean uniq = RelMdUtil.areColumnsDefinitelyUnique(mq, rel, groupKey);
    if (uniq) {
      return mq.getRowCount(rel);
    }

    return null;
  }
}
