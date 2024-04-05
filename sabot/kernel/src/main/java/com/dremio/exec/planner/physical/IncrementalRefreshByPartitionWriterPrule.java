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
package com.dremio.exec.planner.physical;

import com.dremio.exec.planner.IncrementalRefreshByPartitionDeletePlanGenerator;
import com.dremio.exec.planner.logical.IncrementalRefreshByPartitionWriterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.WriterRel;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

public class IncrementalRefreshByPartitionWriterPrule extends WriterPrule {

  public static final RelOptRule INSTANCE = new IncrementalRefreshByPartitionWriterPrule();

  public IncrementalRefreshByPartitionWriterPrule() {
    super(
        RelOptHelper.some(
            IncrementalRefreshByPartitionWriterRel.class,
            Rel.LOGICAL,
            RelOptHelper.any(RelNode.class)),
        "Prel.IncrementalRefreshByPartitionWriterPrule");
  }

  @Override
  public boolean matches(final RelOptRuleCall call) {
    final WriterRel writerRel = call.rel(0);
    return writerRel instanceof IncrementalRefreshByPartitionWriterRel;
  }

  @Override
  protected Prel convertWriter(final WriterRel writer, final RelNode rel) {
    Function<RelNode, Prel> finalize = null;
    DatasetConfig datasetConfig = null;
    final IncrementalRefreshByPartitionWriterRel incrementalRefreshByPartitionWriterRel =
        (IncrementalRefreshByPartitionWriterRel) writer;
    if (incrementalRefreshByPartitionWriterRel.getOldReflection() != null) {
      // This code path is for Incremental Refresh by Partition only
      // We will only enter here if there is an old reflection to incrementally update by partition
      // Otherwise no changes to this function, and we will call convertWriter with null for
      // finalize and datasetConfig as before
      // We will populate arguments finalize and datasetConfig and pass them down to the call to
      // convertWriter
      // This will result in delete sub-plan being added to the plan
      // The delete plan will remove all files belonging to partitions
      // that are modified between the two snapshots in
      // incrementalRefreshByPartitionWriterRel.getSnapshotDiffContext()
      final IncrementalRefreshByPartitionDeletePlanGenerator
          incrementalRefreshByPartitionDeletePlanGenerator =
              new IncrementalRefreshByPartitionDeletePlanGenerator(
                  incrementalRefreshByPartitionWriterRel.getTable(),
                  incrementalRefreshByPartitionWriterRel.getCluster(),
                  incrementalRefreshByPartitionWriterRel.getTraitSet().plus(Prel.PHYSICAL),
                  rel,
                  incrementalRefreshByPartitionWriterRel.getOldReflection().getDataset(),
                  writer.getCreateTableEntry(),
                  incrementalRefreshByPartitionWriterRel.getContext(),
                  incrementalRefreshByPartitionWriterRel.getSnapshotDiffContext());
      finalize = incrementalRefreshByPartitionDeletePlanGenerator.getFinalizeFunc();
      datasetConfig = incrementalRefreshByPartitionWriterRel.getOldReflection().getDatasetConfig();
    }
    return convertWriter(
        writer,
        rel,
        writer.getExpectedInboundRowType(),
        datasetConfig,
        writer.getCreateTableEntry(),
        finalize);
  }
}
