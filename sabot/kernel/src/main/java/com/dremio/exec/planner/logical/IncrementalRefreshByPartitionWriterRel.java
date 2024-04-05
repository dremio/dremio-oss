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

import com.dremio.exec.catalog.DremioTable;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.ops.SnapshotDiffContext;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

/**
 * A class used to signify that the current WriterRel is used for Incremental Refresh by Partition
 * That means that we will possibly have some files deleted in addition to all the added files We
 * use this class to pass some additional parameters needed to generate the delete plan
 */
public class IncrementalRefreshByPartitionWriterRel extends WriterRel {
  private final OptimizerRulesContext context;
  private final DremioTable oldReflection;
  private final RelOptTable table;
  private final SnapshotDiffContext snapshotDiffContext;

  public IncrementalRefreshByPartitionWriterRel(
      final RelOptCluster cluster,
      final RelTraitSet traitSet,
      final RelNode input,
      final CreateTableEntry createTableEntry,
      final RelDataType expectedInboundRowType,
      final OptimizerRulesContext context,
      final DremioTable oldReflection,
      final RelOptTable table,
      final SnapshotDiffContext snapshotDiffContext) {
    super(cluster, traitSet, input, createTableEntry, expectedInboundRowType);
    this.context = context;
    this.oldReflection = oldReflection;
    this.table = table;
    this.snapshotDiffContext = snapshotDiffContext;
  }

  @Override
  public RelNode copy(final RelTraitSet traitSet, final List<RelNode> inputs) {
    return new IncrementalRefreshByPartitionWriterRel(
        getCluster(),
        traitSet,
        sole(inputs),
        getCreateTableEntry(),
        getExpectedInboundRowType(),
        context,
        oldReflection,
        table,
        snapshotDiffContext);
  }

  public OptimizerRulesContext getContext() {
    return context;
  }

  public DremioTable getOldReflection() {
    return oldReflection;
  }

  @Override
  public RelOptTable getTable() {
    return table;
  }

  public SnapshotDiffContext getSnapshotDiffContext() {
    return snapshotDiffContext;
  }
}
