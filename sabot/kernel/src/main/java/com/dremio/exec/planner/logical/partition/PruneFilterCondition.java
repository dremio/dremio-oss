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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

/**
 * PruneFilterCondition
 */
public class PruneFilterCondition {
  private RexNode partitionRange;
  private RexNode nonPartitionRange;
  private RexNode partitionExpression;

  public PruneFilterCondition(RexNode partitionRange, RexNode nonPartitionRange, RexNode partitionExpression) {
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

  public static PruneFilterCondition mergeConditions(RexBuilder builder, List<PruneFilterCondition> conditions) {
    if (conditions.size() == 1) {
      return conditions.get(0);
    }

    List<RexNode> nonPartitionRangeList = new ArrayList<>();
    List<RexNode> partitionRangeList = new ArrayList<>();
    List<RexNode> expressionList = new ArrayList<>();
    for (PruneFilterCondition condition : conditions) {
      RexNode partitionRange = condition.getPartitionRange();
      if (partitionRange != null) {
        partitionRangeList.add(partitionRange);
      }
      RexNode nonPartitionRange = condition.getNonPartitionRange();
      if (nonPartitionRange != null) {
        nonPartitionRangeList.add(nonPartitionRange);
      }
      RexNode partitionExpression = condition.getPartitionExpression();
      if (partitionExpression != null) {
        expressionList.add(partitionExpression);
      }
    }
    return new PruneFilterCondition(
      buildConditionFromList(partitionRangeList, builder),
      buildConditionFromList(nonPartitionRangeList, builder),
      buildConditionFromList(expressionList, builder));
  }

  private static RexNode buildConditionFromList(List<RexNode> conditions, RexBuilder builder) {
    return conditions.size() == 0 ? null : (conditions.size() == 1 ? conditions.get(0) : builder.makeCall(SqlStdOperatorTable.AND, conditions));
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    if (partitionRange != null) {
      builder.append("partition_range_filter:").append(partitionRange.toString()).append(";");
    }
    if (nonPartitionRange != null) {
      builder.append("non_partition_range_filter:").append(nonPartitionRange.toString()).append(";");
    }
    if (partitionExpression != null) {
      builder.append("other_partition_filter:").append(partitionExpression.toString()).append(";");
    }
    return builder.toString();
  }


}
