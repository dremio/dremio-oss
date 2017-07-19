/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.planner.common;

import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.PathSegment;
import com.dremio.common.expression.PathSegment.NameSegment;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.util.ColumnUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Base class for logical scan rel implemented in Dremio.  Should not be used to match rules.
 */
public abstract class OldScanRelBase extends TableScan {

  private static final int MAX_COLUMNS = 1000;

  public static final double DEFAULT_ROW_COUNT_DISCOUNT = 1;

  /****************************************
   * Members used in equals()/hashCode()!
   * If you add a new member that needs to be in equality, add them equals() and hashCode().
   ****************************************/
  protected final RelDataType rowType;
  protected final GroupScan groupScan;
  private final double rowCountDiscount;

  /**
   * Creates an <code>AbstractRelNode</code>.
   *
   * @param cluster
   * @param traitSet
   */
  public OldScanRelBase(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable relOptTable, RelDataType rowType, GroupScan groupScan, double discountRowCount) {
    super(cluster, traitSet, relOptTable);
    this.rowType = Preconditions.checkNotNull(rowType);
    this.groupScan = Preconditions.checkNotNull(groupScan);
    Preconditions.checkArgument(discountRowCount >= 0 && discountRowCount <= 1, "rowCountDiscount cannot be set to " + discountRowCount);
    this.rowCountDiscount = discountRowCount;
  }

  /**
   * All ScanRels should implement its own equals() and hashCode().
   */
  @Override
  public abstract boolean equals(final Object o);
  @Override
  public abstract int hashCode();

  public double getRowCountDiscount() {
    return rowCountDiscount;
  }


  public List<SchemaPath> getColumns() {
    return groupScan.getColumns();
  }

  public GroupScan getGroupScan() {
    return groupScan;
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }


  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(getCluster().getPlanner());
    return rowCountDiscount * this.groupScan.getScanStats(settings).getRecordCount();
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("groupscan", groupScan.getDigest());
  }

  /**
   * Computes the cost honoring column pushdown and relative costs across {@link GroupScan group scans}.
   *
   * The cost model is a function of row count, column and scan factors. Row count may or may not be precise and is
   * obtained from{@link GroupScan#getScanStats(PlannerSettings)}. Column factor is relative to number of columns to
   * scan. Scan factor represents the relative cost of making the scan as compared to other scans kinds.
   *
   * An exact field contributes 1 whole points to column factor. A sub field scan contributes proportionately anywhere
   * from (0, 1], weighting subsequent sub fields less through a logarithmic model.
   */
  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
    final List<SchemaPath> columns = getColumns();
    final boolean isStarQuery = ColumnUtils.isStarQuery(columns);
    final int allColumns = getTable().getRowType().getFieldCount();

    final double columnFactor;
    if (isStarQuery) {
      columnFactor = allColumns;
    } else {
      double runningFactor = 0;
      // Maintain map of root to child segments to determine what fraction of a whole field is projected.
      final Multimap<NameSegment, SchemaPath> subFields = HashMultimap.create();
      for (final SchemaPath column : columns) {
        subFields.put(column.getRootSegment(), column);
      }

      for (Map.Entry<PathSegment.NameSegment, SchemaPath> entry : subFields.entries()) {
        final PathSegment.NameSegment root = entry.getKey();
        final SchemaPath field = entry.getValue();
        final String rootPath = root.getPath();
        final boolean entireColSelected = rootPath.equalsIgnoreCase(field.getAsUnescapedPath());
        if (entireColSelected) {
          runningFactor += 1;
        } else {
          final RelDataType dataType = getRowType().getField(rootPath, false, false).getType();
          final int subFieldCount = dataType.isStruct() ? dataType.getFieldCount() : 1;
          // sandwich impact of this projection between (0, 1]
          final double impact = Math.log10(1 + (Math.min(1, subFieldCount/MAX_COLUMNS) * 9));
          runningFactor += impact;
        }
      }
      columnFactor = runningFactor;
    }

    final double estimatedCount = estimateRowCount(mq);
    // If the estimatedCount is actually 0, then make it 1, so that at least, we choose the scan that
    // has fewer columns pushed down since all the cost scales with rowCount.
    final double rowCount = Math.max(1, estimatedCount);
    final double scanFactor = getGroupScan().getScanCostFactor().getFactor();
    final double cpuCost = rowCount * columnFactor * scanFactor;
    final double ioCost = cpuCost;

    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return planner.getCostFactory().makeCost(estimatedCount, cpuCost, ioCost);
    }

    final double networkCost = ioCost;
    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.
    // In the future we might consider alternative scans that go against projections or
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost.
    final DremioCost.Factory costFactory = (DremioCost.Factory)planner.getCostFactory();
    return costFactory.makeCost(estimatedCount, cpuCost, ioCost, networkCost);
  }

}
