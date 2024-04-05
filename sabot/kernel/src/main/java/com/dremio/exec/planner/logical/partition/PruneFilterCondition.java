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
package com.dremio.exec.planner.logical.partition;

import static com.dremio.exec.planner.common.MoreRelOptUtil.getInputRewriterFromProjectedFields;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.record.BatchSchema;
import com.google.common.base.Preconditions;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

/** PruneFilterCondition */
public class PruneFilterCondition {
  private final RexNode partitionRange;
  private final RexNode nonPartitionRange;
  private final RexNode partitionExpression;

  public PruneFilterCondition(
      RexNode partitionRange, RexNode nonPartitionRange, RexNode partitionExpression) {
    // TODO use alwaysTrue instead of null to follow the calcite convention
    Preconditions.checkArgument(null == partitionRange || !partitionRange.isAlwaysTrue());
    Preconditions.checkArgument(null == nonPartitionRange || !nonPartitionRange.isAlwaysTrue());
    Preconditions.checkArgument(null == partitionExpression || !partitionExpression.isAlwaysTrue());
    this.partitionRange = partitionRange;
    this.nonPartitionRange = nonPartitionRange;
    this.partitionExpression = partitionExpression;
  }

  public RexNode getPartitionRange() {
    return partitionRange;
  }

  public RexNode getNonPartitionRange() {
    return nonPartitionRange;
  }

  public RexNode getPartitionExpression() {
    return partitionExpression;
  }

  public boolean isEmpty() {
    return partitionRange == null && nonPartitionRange == null && partitionExpression == null;
  }

  public PruneFilterCondition applyProjection(
      List<SchemaPath> projection,
      RelDataType rowType,
      RelOptCluster cluster,
      BatchSchema batchSchema) {
    final PrelUtil.InputRewriter inputRewriter =
        getInputRewriterFromProjectedFields(projection, rowType, batchSchema, cluster);
    RexNode newPartitionRange =
        getPartitionRange() != null ? getPartitionRange().accept(inputRewriter) : null;
    RexNode newNonPartitionRange =
        getNonPartitionRange() != null ? getNonPartitionRange().accept(inputRewriter) : null;
    RexNode newPartitionExpression =
        getPartitionExpression() != null ? getPartitionExpression().accept(inputRewriter) : null;
    return new PruneFilterCondition(
        newPartitionRange, newNonPartitionRange, newPartitionExpression);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (null != partitionRange) {
      builder.append("partition_range_filter:").append(partitionRange).append(";");
    }
    if (null != nonPartitionRange) {
      builder.append("non_partition_range_filter:").append(nonPartitionRange).append(";");
    }
    if (null != partitionExpression) {
      builder.append("other_partition_filter:").append(partitionExpression).append(";");
    }
    return builder.toString();
  }
}
