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

import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.MetadataHandlerProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class DremioRelMetadataQuery extends RelMetadataQuery{
  private static final DremioRelMetadataQuery PROTOTYPE =
      new DremioRelMetadataQuery(DremioMetadataHandlerProvider.INSTANCE);

  public static final RelMetadataQuerySupplier QUERY_SUPPLIER =
      () -> new DremioRelMetadataQuery(PROTOTYPE);

  private DremioRelMetadataQuery(MetadataHandlerProvider metadataHandlerProvider) {
    super(metadataHandlerProvider);
  }

  private DremioRelMetadataQuery(RelMetadataQuery prototype) {
    super(prototype);
  }

  @Override
  public ImmutableList<RelCollation> collations(RelNode rel) {
    ImmutableList<RelCollation> colList = super.collations(rel);
    return (colList.size() == 1 && colList.get(0).equals(RelCollations.EMPTY))
        ? ImmutableList.of()
        : colList;
  }

  @Override
  public RelOptCost getCumulativeCost(RelNode rel) {
    RelOptCost cost = super.getCumulativeCost(rel);

    return cost == null
      ? DremioCost.ZERO
      : cost;
  }

  @Override
  public RelOptCost getNonCumulativeCost(RelNode rel) {
    RelOptCost cost = super.getNonCumulativeCost(rel);

    return cost == null
        ? DremioCost.ZERO
        : cost;
  }

  @Override
  public Set<RelColumnOrigin> getColumnOrigins(RelNode rel, int column) {
    Set<RelColumnOrigin> columnOrigins = super.getColumnOrigins(rel, column);
    return columnOrigins == null
      ? ImmutableSet.of()
      : columnOrigins;
  }

  /**
   * Ensures only one instance of DremioRelMetadataQuery is created for statistics
   */
  public static RelMetadataQuerySupplier getSupplier(StatisticsService service) {
    DremioMetadataHandlerProvider provider =
        DremioMetadataHandlerProvider.createMetadataProviderWithStatics(service);
    DremioRelMetadataQuery prototype =
      new DremioRelMetadataQuery(provider);
    return () -> new DremioRelMetadataQuery(prototype);
  }
}
