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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.planner.common.FlattenRelBase;
import com.dremio.exec.planner.common.JdbcRelBase;
import com.dremio.exec.planner.common.JoinRelBase;
import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.AggPrelBase;
import com.dremio.exec.planner.physical.BroadcastExchangePrel;
import com.dremio.exec.planner.physical.FlattenPrel;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.planner.physical.TableFunctionPrel;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.dfs.FilterableScan;
import com.dremio.exec.store.sys.statistics.StatisticsService;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.NamespaceKey;
import com.google.common.base.Throwables;

public class RelMdRowCount extends org.apache.calcite.rel.metadata.RelMdRowCount {
  private static final Logger logger = LoggerFactory.getLogger(RelMdRowCount.class);
  private static final RelMdRowCount INSTANCE = new RelMdRowCount(StatisticsService.NO_OP);
  private static final Double decreaseSelectivityForInexactFilters = 0.999;
  public static final RelMetadataProvider SOURCE = ReflectiveRelMetadataProvider.reflectiveSource(BuiltInMethod.ROW_COUNT.method, INSTANCE);

  private final StatisticsService statisticsService;
  private boolean isNoOp;

  public RelMdRowCount(StatisticsService statisticsService) {
    this.statisticsService = statisticsService;
    this.isNoOp = statisticsService == StatisticsService.NO_OP;
  }

  @Override
  public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
    ImmutableBitSet groupKey = rel.getGroupSet();
    if (groupKey.isEmpty()) {
      return 1.0;
    } else if (!DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      return rel.estimateRowCount(mq);
    } else if (rel instanceof AggPrelBase && ((AggPrelBase) rel).getOperatorPhase() == AggPrelBase.OperatorPhase.PHASE_1of2) {
      // Phase 1 Aggregate would return rows in the range [NDV, input_rows]. Hence, use the
      // existing estimate of 1/10 * input_rows
      double rowCount = mq.getRowCount(rel.getInput()) / 10;
      try {
        Double ndv = mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        // Use max of NDV and input_rows/10
        if (ndv != null) {
          rowCount = Math.max(ndv, rowCount);
        }
        // Grouping sets multiply
        rowCount *= rel.getGroupSets().size();

        if(rowCount >= mq.getRowCount(rel.getInput())){
          // our estimation has failed completely
          return rel.estimateRowCount(mq);
        }
        return rowCount;
      } catch (Exception ex) {
        logger.debug("Failed to get row count of aggregate. Fallback to default estimation", ex);
        return rel.estimateRowCount(mq);
      }
    }

    try {
      Double distinctRowCount = mq.getDistinctRowCount(rel.getInput(), groupKey, null);
      if (distinctRowCount == null) {
        return rel.estimateRowCount(mq);
      }

      Double rowCount = (double) distinctRowCount * (double) rel.getGroupSets().size();
      if(rowCount >= mq.getRowCount(rel.getInput())){
        // our estimation has failed completely
        return rel.estimateRowCount(mq);
      }
      return rowCount;
    } catch (Exception ex) {
      logger.debug("Failed to get row count of aggregate. Fallback to default estimation", ex);
      return rel.estimateRowCount(mq);
    }
  }

  @Override
  public Double getRowCount(Join rel, RelMetadataQuery mq) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      Double rowCount = estimateJoinRowCountWithStatistics(rel, mq);
      if (rowCount != null) {
        return rowCount;
      }
    }
    return estimateRowCount(rel, mq);
  }

  private Double getDistinctCountForJoinChild(RelMetadataQuery mq, RelNode rel, ImmutableBitSet cols) {
    if (cols.asList().stream().anyMatch(col -> {
      Set<RelColumnOrigin> columnOrigins = mq.getColumnOrigins(rel, col);
      if (columnOrigins != null) {
        for (RelColumnOrigin columnOrigin : columnOrigins) {
          final RelOptTable originTable = columnOrigin.getOriginTable();
          final List<String> fieldNames = originTable.getRowType().getFieldNames();
          final String columnName = fieldNames.get(columnOrigin.getOriginColumnOrdinal());
          if (statisticsService.getNDV(columnName, new NamespaceKey(originTable.getQualifiedName())) != null) {
            return true;
          }
        }
      }
      return false;
    })) {
      return mq.getDistinctRowCount(rel, cols, null);
    }
    return null;
  }

  public Double estimateJoinRowCountWithStatistics(Join rel, RelMetadataQuery mq) {
    final RexNode condition = rel.getCondition();
    if (condition.isAlwaysTrue()) {
      return null;
    }

    final List<Integer> leftKeys = new ArrayList<>();
    final List<Integer> rightKeys = new ArrayList<>();
    final RexNode remaining = RelOptUtil.splitJoinCondition(rel.getLeft(), rel.getRight(), condition, leftKeys, rightKeys, new ArrayList<>());
    final ImmutableBitSet.Builder leftBuilder = ImmutableBitSet.builder();
    final ImmutableBitSet.Builder rightBuilder = ImmutableBitSet.builder();
    leftKeys.forEach(leftBuilder::set);
    rightKeys.forEach(rightBuilder::set);
    final ImmutableBitSet leftCols = leftBuilder.build();
    final ImmutableBitSet rightCols = rightBuilder.build();

    final RelNode left = rel.getLeft();
    final RelNode right = rel.getRight();
    final Double leftNdv = getDistinctCountForJoinChild(mq, left, leftCols);
    final Double rightNdv = getDistinctCountForJoinChild(mq, right, rightCols);
    final Double leftRowCount = mq.getRowCount(left);
    final Double rightRowCount = mq.getRowCount(right);

    if (leftNdv == null || rightNdv == null
      || leftNdv == 0 || rightNdv == 0
      || leftRowCount == null || rightRowCount == null) {
      // fallback to largest estimate
     return RelMdUtil.getJoinRowCount(mq, rel, condition);
    }

    final Double selectivity = mq.getSelectivity(rel, remaining);
    double remainingSelectivity = selectivity == null ? 1.0D : selectivity;

    final double minNdv = Math.min(leftNdv, rightNdv);
    double leftSelectivity = (minNdv / leftNdv) * remainingSelectivity;
    double rightSelectivity = (minNdv / rightNdv) * remainingSelectivity;
    double innerJoinCardinality = ((minNdv * leftRowCount * rightRowCount) / (leftNdv * rightNdv)) * remainingSelectivity;
    double leftMismatchCount = (1 - leftSelectivity) * leftRowCount;
    double rightMismatchCount = (1 - rightSelectivity) * rightRowCount;
    switch (rel.getJoinType()) {
      case INNER:
        if (leftNdv * rightNdv == 0) {
          return null;
        }
        return innerJoinCardinality;
      case LEFT:
        double rightMatches = rightRowCount / rightNdv;
        return (leftSelectivity * leftRowCount * rightMatches) + leftMismatchCount;
      case RIGHT:
        double leftMatches = leftRowCount / leftNdv;
        return (rightSelectivity * rightRowCount * leftMatches) + rightMismatchCount;
      case FULL:
        return innerJoinCardinality + leftMismatchCount + rightMismatchCount;
      default:
        return null;
    }
  }

  /**
   * DX-35733: Need to better estimate join row count for self joins, which is usually not a key-
   * foreign key join, so for self joins we adopt Calcite's default implementation.
   *
   * DX-3859:  Need to make sure that join row count is calculated in a reasonable manner. Calcite's
   * default implementation is leftRowCount * rightRowCount * discountBySelectivity, which is too
   * large (cartesian join). Since we do not support cartesian join, by default we assume a join is
   * key-foreign key join. {@link #estimateForeignKeyJoinRowCount(Join, RelMetadataQuery)}
   */
  public static double estimateRowCount(Join rel, RelMetadataQuery mq) {
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(rel.getCluster().getPlanner());
    if (isSelfJoin(rel, mq) && plannerSettings!=null && plannerSettings.isNewSelfJoinCostEnabled()) {
      // Calcite's default implementation
      return RelMdUtil.getJoinRowCount(mq, rel, rel.getCondition());
    } else {
      // Dremio's default implementation
      return estimateForeignKeyJoinRowCount(rel, mq);
    }
  }

  /**
   * A join is self join if all join keys are originated from the same table.
   */
  private static boolean isSelfJoin(Join rel, RelMetadataQuery mq) {
    // Find left keys and right keys of equi-join condition
    List<Integer> leftKeys = new ArrayList<>();
    List<Integer> rightKeys = new ArrayList<>();
    RelOptUtil.splitJoinCondition(rel.getLeft(), rel.getRight(), rel.getCondition(),
      leftKeys, rightKeys, new ArrayList<>());
    if (leftKeys.isEmpty()) {
      return false;
    }

    // Check each pair of join key
    for (int i = 0; i < leftKeys.size(); ++i) {
      int leftKey = leftKeys.get(i);
      int rightKey = rightKeys.get(i);
      RelColumnOrigin leftColumnOrigin = mq.getColumnOrigin(rel.getLeft(), leftKey);
      RelColumnOrigin rightColumnOrigin = mq.getColumnOrigin(rel.getRight(), rightKey);
      if (leftColumnOrigin == null || rightColumnOrigin == null) {
        return false;
      }
      List<String> leftTableName = leftColumnOrigin.getOriginTable().getQualifiedName();
      List<String> rightTableName = rightColumnOrigin.getOriginTable().getQualifiedName();
      if (!leftTableName.equals(rightTableName)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Estimate join row count for a key-foreign key join, basically just take the maximum of the two
   * join input row counts.
   */
  private static Double estimateForeignKeyJoinRowCount(Join rel, RelMetadataQuery mq) {
    double rightJoinFactor = 1.0;

    RexNode condition = rel.getCondition();
    if (condition.isAlwaysTrue()) {
      // Cartesian join is only supported for NLJ. If join type is right, make it more expensive
      if (rel.getJoinType() == JoinRelType.RIGHT) {
        rightJoinFactor = 2.0;
      }
      return RelMdUtil.getJoinRowCount(mq, rel, condition) * rightJoinFactor;
    }

    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(rel.getCluster().getPlanner());
    double filterMinSelectivityEstimateFactor = plannerSettings == null ?
      PlannerSettings.DEFAULT_FILTER_MIN_SELECTIVITY_ESTIMATE_FACTOR :
      plannerSettings.getFilterMinSelectivityEstimateFactor();
    double filterMaxSelectivityEstimateFactor = plannerSettings == null ?
      PlannerSettings.DEFAULT_FILTER_MAX_SELECTIVITY_ESTIMATE_FACTOR :
      plannerSettings.getFilterMaxSelectivityEstimateFactor();

    final RexNode remaining;
    if (rel instanceof JoinRelBase) {
      remaining = ((JoinRelBase) rel).getRemaining();
    } else {
      remaining = RelOptUtil.splitJoinCondition(rel.getLeft(), rel.getRight(), condition, new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    double selectivity = mq.getSelectivity(rel, remaining);
    if (!remaining.isAlwaysFalse()) {
      // Cap selectivity at filterMinSelectivityEstimateFactor unless it is always FALSE
      if (selectivity < filterMinSelectivityEstimateFactor) {
        selectivity = filterMinSelectivityEstimateFactor;
      }
    }

    if (!remaining.isAlwaysTrue()) {
      // Cap selectivity at filterMaxSelectivityEstimateFactor unless it is always TRUE
      if (selectivity > filterMaxSelectivityEstimateFactor) {
        selectivity = filterMaxSelectivityEstimateFactor;
      }
      // Make right join more expensive for inequality join condition (logical phase)
      if (rel.getJoinType() == JoinRelType.RIGHT) {
        rightJoinFactor = 2.0;
      }
    }

    return selectivity * Math.max(mq.getRowCount(rel.getLeft()), mq.getRowCount(rel.getRight())) * rightJoinFactor;
  }

  public Double getRowCount(MultiJoin rel, RelMetadataQuery mq) {
    if (rel.getJoinFilter().isAlwaysTrue() &&
      RexUtil.composeConjunction(rel.getCluster().getRexBuilder(), rel.getOuterJoinConditions(), false).isAlwaysTrue()) {
      double rowCount = 1;
      for (RelNode input : rel.getInputs()) {
        rowCount *= mq.getRowCount(input);
      }
      return rowCount;
    } else {
      double max = 1;
      for (RelNode input : rel.getInputs()) {
        max = Math.max(max, mq.getRowCount(input));
      }
      return max;
    }
  }

  public Double getRowCount(FlattenRelBase flatten, RelMetadataQuery mq) {
    return flatten.estimateRowCount(mq);
  }

  public Double getRowCount(FlattenPrel flatten, RelMetadataQuery mq) {
    return flatten.estimateRowCount(mq);
  }

  public Double getRowCount(LimitRelBase limit, RelMetadataQuery mq) {
    return limit.estimateRowCount(mq);
  }

  public Double getRowCount(JdbcRelBase jdbc, RelMetadataQuery mq) {
    return jdbc.getSubTree().estimateRowCount(mq);
  }

  public Double getRowCount(BroadcastExchangePrel rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }

  @Override
  public Double getRowCount(Filter rel, RelMetadataQuery mq) {
    return rel.estimateRowCount(mq);
  }

  public Double getRowCount(ScanRelBase rel, RelMetadataQuery mq) {
    try {
      double splitRatio = rel.getTableMetadata() != null ? rel.getTableMetadata().getSplitRatio() : 1.0d;
      if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
        return getRowCount((TableScan) rel, mq) * splitRatio * rel.getObservedRowcountAdjustment();
      }
      double rowCount = rel.getTable().getRowCount();
      if (DremioRelMdUtil.isRowCountStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
        Double rowCountFromStat = getRowCountFromTableMetadata(rel);
        if (rowCountFromStat != null) {
          rowCount = rowCountFromStat;
        }
      }

      double partitionFilterFactor = 1.0d;
      if ((rel instanceof FilterableScan) && ((FilterableScan) rel).getPartitionFilter() != null) {
        final PlannerSettings plannerSettings  = PrelUtil.getPlannerSettings(rel.getCluster().getPlanner());
        partitionFilterFactor *= plannerSettings == null ? PlannerSettings.DEFAULT_PARITTION_FILTER_FACTOR : plannerSettings.getPartitionFilterFactor();
      }

      return rel.getFilterReduction() * rowCount * splitRatio * rel.getObservedRowcountAdjustment() * partitionFilterFactor;
    } catch (NamespaceException ex) {
      logger.warn("Failed to get split ratio from table metadata, {}", rel.getTableMetadata().getName());
      throw Throwables.propagate(ex);
    }
  }

  private Double getSelectivityFromTableScanWithScanFilter(TableScan rel, RelMetadataQuery mq, ScanFilter scanFilter){
    Double  selectivity = mq.getSelectivity(rel, scanFilter.getExactRexFilter());
    if(scanFilter.getExactRexFilter() == null || !scanFilter.getExactRexFilter().equals(scanFilter.getRexFilter())){
      // We make the selectivity with inExact Filters lower so that the planner chooses the plan with the InExact Filters pushed to the TableScan
      // ToDo: Handle this part in TableScan costing
      selectivity = selectivity * decreaseSelectivityForInexactFilters;
    }
    return selectivity;
  }


  @Override
  public Double getRowCount(TableScan rel, RelMetadataQuery mq) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      Double rowCount = getRowCountFromTableMetadata(rel);
      double selectivity = 1.0;
      if (rel instanceof FilterableScan) {
        FilterableScan filterableScan = (FilterableScan) rel;
        if (filterableScan.getFilter() != null) {
          selectivity *= getSelectivityFromTableScanWithScanFilter(rel, mq, ((FilterableScan) rel).getFilter());
        }
        if (filterableScan.getPartitionFilter() != null) {
          // todo DX-38641 - get selectivity through stat
          final PlannerSettings plannerSettings  = PrelUtil.getPlannerSettings(rel.getCluster().getPlanner());
          selectivity *= plannerSettings == null ? PlannerSettings.DEFAULT_PARITTION_FILTER_FACTOR : plannerSettings.getPartitionFilterFactor();
        }
      } else if (rel instanceof ScanPrelBase && ((ScanPrelBase) rel).hasFilter() && ((ScanPrelBase) rel).getFilter() != null) {
        selectivity = getSelectivityFromTableScanWithScanFilter(rel, mq, ((ScanPrelBase) rel).getFilter());
      }
      return rowCount == null ? rel.getTable().getRowCount() : selectivity * rowCount;
    }
    return ((TableScan) rel).estimateRowCount(mq);
  }

  public Double getRowCount(TableFunctionPrel rel, RelMetadataQuery mq) {
    if (DremioRelMdUtil.isStatisticsEnabled(rel.getCluster().getPlanner(), isNoOp)) {
      Double rowCount = getRowCountFromTableMetadata(rel);
      return rowCount == null ? rel.estimateRowCount(mq) : rowCount;
    }
    return rel.estimateRowCount(mq);
  }

  private Double getRowCountFromTableMetadata(RelNode rel) {
    TableMetadata tableMetadata = null;
    if (rel instanceof TableFunctionPrel) {
      tableMetadata = ((TableFunctionPrel) rel).getTableMetadata();
    } else if (rel instanceof ScanRelBase) {
      tableMetadata = ((ScanRelBase) rel).getTableMetadata();
    }

    if (tableMetadata == null) {
      return null;
    }

    try {
      Long rowCount = statisticsService.getRowCount(tableMetadata.getName());
      if (rowCount != null) {
        return rowCount.doubleValue();
      } else {
        return null;
      }
    } catch (Exception ex) {
      logger.debug("Failed to get row count. Fallback to default estimation", ex);
      return null;
    }
  }
}
