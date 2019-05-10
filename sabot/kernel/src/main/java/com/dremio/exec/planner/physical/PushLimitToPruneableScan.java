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

package com.dremio.exec.planner.physical;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.PruneableScan;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.PartitionChunkMetadata;

/**
 * This rule push limits to pruneable scan by removing extra partition
 * which goes above the number of required records
 */
public class PushLimitToPruneableScan extends Prule {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(PushLimitToPruneableScan.class);

  public static final RelOptRule INSTANCE = new PushLimitToPruneableScan();

  private PushLimitToPruneableScan() {
    super(operand(LimitPrel.class, operand(ScanPrelBase.class, any())), "PushLimitToPruneableScan");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    // Check that the second operator is a pruneable scan with no associated filter
    // A filter inside scan could impact rowcount and cause wrong result if less splits were present
    return !((ScanPrelBase) call.rel(1)).hasFilter() && call.rel(1) instanceof PruneableScan;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final LimitPrel limit = (LimitPrel) call.rel(0);
    final ScanPrelBase scan = (ScanPrelBase) call.rel(1);

    // First offset to include into results (inclusive). Null implies it is starting from offset 0
    final int offset = getIntValue(limit.getOffset());
    final int fetch = getIntValue(limit.getFetch());

    // Do not go over list of partitions if fetch is 0
    if (fetch == 0) {
      call.transformTo(new EmptyPrel(scan.getCluster(), scan.getTraitSet(), scan.getRowType(), scan.getProjectedSchema()));
      return;
    }

    final List<PartitionChunkMetadata> newChunks = new ArrayList<>();

    final TableMetadata dataset = scan.getTableMetadata();
    final Iterator<PartitionChunkMetadata> chunks = dataset.getSplits();


    // Find the new start offset
    int chunksSize = 0;
    int newOffset = offset;
    int total = 0;
    while(chunks.hasNext()) {
      chunksSize++;
      final PartitionChunkMetadata chunk = chunks.next();
      final long rowCount = chunk.getRowCount();

      if (total + rowCount <= newOffset) {
        newOffset -= rowCount;
        continue;
      }

      if (total >= newOffset + fetch) {
        break;
      }

      total += rowCount;
      newChunks.add(chunk);
    }

    // Do not recreate if newChunks got all the elements from chunks
    if (!chunks.hasNext() && newChunks.size() == chunksSize) {
      return;
    }

    // Create a new scan with the reduced list of partition chunks
    // if no chunk at all, use empty
    final RelNode newScan;
    if (newChunks.isEmpty()) {
      newScan = new EmptyPrel(scan.getCluster(), scan.getTraitSet(), scan.getRowType(), scan.getProjectedSchema());
      call.transformTo(newScan);
      return;
    }

    try {
      newScan = ((PruneableScan) scan).applyDatasetPointer(dataset.prune(newChunks));
    } catch (NamespaceException e) {
      // Nothing we can do except not applying the rule and mentioning it in the logs
      LOGGER.warn("Namespace exception while pruning chunks", e);
      return;
    }

    final RelNode newLimit = new LimitPrel(limit.getCluster(), limit.getTraitSet(), newScan, call.builder().literal(newOffset), limit.getFetch());
    call.transformTo(newLimit);
  }

  private static int getIntValue(RexNode value) {
    // Return the literal value. Replace with 0 if null or negative
    return Math.max(0, Optional.ofNullable(value).map(RexLiteral::intValue).orElse(0));
  }

}
