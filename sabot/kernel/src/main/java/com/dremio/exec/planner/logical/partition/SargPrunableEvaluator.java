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

import static com.dremio.exec.planner.physical.PlannerSettings.ENABLE_ICEBERG_NATIVE_PARTITION_PRUNER;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.TableMetadata;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.iceberg.PartitionStatsEntry;
import org.apache.iceberg.StructLike;

public abstract class SargPrunableEvaluator {
  private final PartitionStatsBasedPruner pruner;

  /**
   * Creates a new instance of specific implementation of SargPrunableEvaluator based on support
   * keys enabled
   *
   * @param pruner PartitionStatsBasedPruner this is called from
   * @param usedIndexes a function to get used column indexes from a Rex Node
   * @param projectedColumns what partition columns are projected
   * @param rexVisitor FindSimpleFilters to use to parse conditions into sarge prunable parts
   * @param rexConditions conditions to be used for pruning
   * @param rowType the rowtype of the table we are pruning
   * @param cluster RelOptCluster cluster to use
   * @return a new instance of specific implementation of SargPrunableEvaluator based on support
   *     keys enabled
   */
  public static SargPrunableEvaluator newInstance(
      final PartitionStatsBasedPruner pruner,
      final Function<RexNode, List<Integer>> usedIndexes,
      final List<SchemaPath> projectedColumns,
      final FindSimpleFilters rexVisitor,
      final ImmutableList<RexCall> rexConditions,
      final RelDataType rowType,
      final RelOptCluster cluster) {
    if (pruner
        .optimizerContext
        .getPlannerSettings()
        .getOptions()
        .getOption(ENABLE_ICEBERG_NATIVE_PARTITION_PRUNER)) {
      return IcebergNativeEvaluator.buildSargPrunableConditions(
          pruner, rexConditions, rowType, cluster, rexVisitor);
    }
    return ConditionsByColumn.buildSargPrunableConditions(
        pruner, usedIndexes, projectedColumns, rexVisitor, rexConditions);
  }

  SargPrunableEvaluator(final PartitionStatsBasedPruner pruner) {
    this.pruner = pruner;
  }

  /**
   * Parse rexConditions, build a list of conditions suitable for eval pruning from them
   *
   * @param rexConditions list of conditions to consider
   * @param rexVisitor visitor to use to extract operands
   * @return list of UnprocessedCondition that are suitable for further processing for eval pruning
   */
  protected static List<UnprocessedCondition> buildUnprocessedConditions(
      final ImmutableList<RexCall> rexConditions, final FindSimpleFilters rexVisitor) {
    final List<UnprocessedCondition> unprocessedConditions = new ArrayList<>();
    for (final RexCall condition : rexConditions) {
      final ImmutableList<RexNode> ops = condition.operands;
      if (ops.size() != 2) {
        continue; // We're only interested in conditions with two operands
      }

      final FindSimpleFilters.StateHolder a = ops.get(0).accept(rexVisitor);
      final FindSimpleFilters.StateHolder b = ops.get(1).accept(rexVisitor);
      if (a.getType() == FindSimpleFilters.Type.LITERAL
          && b.getType() == FindSimpleFilters.Type.INPUT) {
        // e.g. '2020' = year
        unprocessedConditions.add(new UnprocessedCondition(condition, b, a));
      } else if (a.getType() == FindSimpleFilters.Type.INPUT
          && b.getType() == FindSimpleFilters.Type.LITERAL) {
        // e.g. year = '2020'
        unprocessedConditions.add(new UnprocessedCondition(condition, a, b));
      }
    }
    return unprocessedConditions;
  }

  public abstract boolean hasConditions();

  /**
   * Check if a partition qualifies given the current filter for Eval Pruning
   *
   * @param partitionData partition to consider
   * @return true if current partition qualifies, false otherwise
   */
  public abstract boolean isRecordMatch(final StructLike partitionData);

  /**
   * Find the number of records and files that qualify after partition pruning
   *
   * @param tableMetadata the table metadata to use
   * @return pair contain number of records and number of files that qualify
   */
  public Pair<Long, Long> evaluateWithSarg(final TableMetadata tableMetadata) {
    pruner.timer.start();
    long qualifiedCount = 0, qualifiedFileCount = 0;
    while (pruner.getStatsEntryIterator().hasNext()) {
      final PartitionStatsEntry statsEntry = pruner.getStatsEntryIterator().next();
      final StructLike partitionData = statsEntry.getPartition();
      if (isRecordMatch(partitionData)) {
        final long statsEntryRecordCount = statsEntry.getRecordCount();
        final long statsEntryFileCount = statsEntry.getFileCount();
        qualifiedCount += statsEntryRecordCount;
        qualifiedFileCount += statsEntryFileCount;

        if (statsEntryRecordCount < 0 || qualifiedCount < 0) {
          qualifiedCount = tableMetadata.getApproximateRecordCount();
          RecordPruner.getLogger()
              .info(
                  "Record count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}",
                  pruner.getFileLocation(),
                  this);
          break;
        }

        if (statsEntryFileCount < 0 || qualifiedFileCount < 0) {
          qualifiedFileCount =
              tableMetadata
                  .getDatasetConfig()
                  .getReadDefinition()
                  .getManifestScanStats()
                  .getRecordCount();
          RecordPruner.getLogger()
              .info(
                  "File count overflowed while evaluating partition filter. Partition stats file {}. Partition Expression {}",
                  pruner.getFileLocation(),
                  this);
          break;
        }
      }
    }
    RecordPruner.getLogger()
        .debug(
            "Elapsed time to find surviving records: {}ms",
            pruner.timer.elapsed(TimeUnit.MILLISECONDS));
    pruner.timer.reset();

    return Pair.of(qualifiedCount, qualifiedFileCount);
  }

  public PartitionStatsBasedPruner getPruner() {
    return pruner;
  }
}

class UnprocessedCondition {
  private final RexCall condition;
  private final FindSimpleFilters.StateHolder column;
  private final FindSimpleFilters.StateHolder constant;

  UnprocessedCondition(
      final RexCall condition,
      final FindSimpleFilters.StateHolder column,
      final FindSimpleFilters.StateHolder constant) {
    this.condition = condition;
    this.column = column;
    this.constant = constant;
  }

  public RexCall getCondition() {
    return condition;
  }

  public FindSimpleFilters.StateHolder getColumn() {
    return column;
  }

  public FindSimpleFilters.StateHolder getConstant() {
    return constant;
  }
}
