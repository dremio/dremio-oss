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
package com.dremio.exec.planner;

import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.SnapshotDiffContext;
import com.dremio.exec.physical.config.ImmutableManifestScanFilters;
import com.dremio.exec.planner.logical.CreateTableEntry;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.iceberg.IcebergScanPlanBuilder;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;

/**
 * A class that generates a plan to delete files that are no longer needed from the reflection when
 * we do Incremental Refresh by Partition.
 */
public class IncrementalRefreshByPartitionDeletePlanGenerator extends TableManagementPlanGenerator {
  private final IcebergScanPlanBuilder planBuilder;
  private final SnapshotDiffContext snapshotDiffContext;

  public IncrementalRefreshByPartitionDeletePlanGenerator(
      final RelOptTable table,
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelNode input,
      final TableMetadata tableMetadata,
      final CreateTableEntry createTableEntry,
      final OptimizerRulesContext context,
      final SnapshotDiffContext snapshotDiffContext) {
    super(table, cluster, traitSet, input, tableMetadata, createTableEntry, context);
    this.snapshotDiffContext = snapshotDiffContext;
    final ImmutableManifestScanFilters.Builder manifestScanFiltersBuilder =
        new ImmutableManifestScanFilters.Builder();
    this.planBuilder =
        new IcebergScanPlanBuilder(
            cluster,
            traitSet,
            table,
            tableMetadata,
            null,
            context,
            manifestScanFiltersBuilder.build(),
            null);
  }

  @Override
  public Prel getPlan() {
    return null;
  }

  public Function<RelNode, Prel> getFinalizeFunc() {
    return manifestWriterPlan -> {
      try {
        return getMetadataWriterPlan(
            deleteDataFilePlan(planBuilder, snapshotDiffContext), manifestWriterPlan);
      } catch (final InvalidRelException e) {
        throw new RuntimeException(e);
      }
    };
  }
}
