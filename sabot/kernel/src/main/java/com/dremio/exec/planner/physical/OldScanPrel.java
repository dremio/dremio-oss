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
package com.dremio.exec.planner.physical;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.ScanStats;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.collect.ImmutableList;

@Deprecated
public class OldScanPrel extends AbstractRelNode implements OldScanPrelBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory
      .getLogger(OldScanPrel.class);

  protected final GroupScan groupScan;
  private final RelDataType rowType;
  private final List<String> qualifiedTableName;

  public OldScanPrel(RelOptCluster cluster, RelTraitSet traits,
      GroupScan groupScan, RelDataType rowType, final List<String> qualifiedTableName) {
    super(cluster, traits);
    this.groupScan = getCopy(groupScan);
    this.rowType = rowType;
    this.qualifiedTableName = ImmutableList.copyOf(qualifiedTableName);
  }

  public List<String> getQualifiedTableName() {
    return qualifiedTableName;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new OldScanPrel(this.getCluster(), traitSet, groupScan,
        this.rowType, this.qualifiedTableName);
  }

  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs, GroupScan groupScan, RelDataType rowType) {
    return new OldScanPrel(this.getCluster(), traitSet, groupScan,
      rowType, this.qualifiedTableName);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return new OldScanPrel(this.getCluster(), this.getTraitSet(), getCopy(groupScan),
        this.rowType, this.qualifiedTableName);
  }

  private static GroupScan getCopy(GroupScan scan){
    try {
      return (GroupScan) scan.getNewWithChildren(Collections.<PhysicalOperator>emptyList());
    } catch (ExecutionSetupException e) {
      throw new RuntimeException("Unexpected failure while coping node.", e);
    }
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator)
      throws IOException {
    return creator.addMetadata(this, groupScan);
  }

  @Override
  public GroupScan getGroupScan() {
    return groupScan;
  }

  public static OldScanPrel create(RelNode old, RelTraitSet traitSets,
      GroupScan scan, RelDataType rowType, List<String> qualifiedTableName) {
    return new OldScanPrel(old.getCluster(), traitSets, getCopy(scan), rowType, qualifiedTableName);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
        .item("groupscan", groupScan.getDigest())
        .item("table", qualifiedTableName);
  }

  @Override
  public RelDataType deriveRowType() {
    return this.rowType;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(getCluster());
    return this.groupScan.getScanStats(settings).getRecordCount();
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, RelMetadataQuery relMetadataQuery) {
    final PlannerSettings settings = PrelUtil.getPlannerSettings(planner);
    final ScanStats stats = this.groupScan.getScanStats(settings);
    final int columnCount = this.getRowType().getFieldCount();

    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return planner.getCostFactory().makeCost(stats.getRecordCount() * columnCount, stats.getCpuCost(), stats.getDiskCost());
    }

    // double rowCount = RelMetadataQuery.getRowCount(this);
    double rowCount = stats.getRecordCount();

    // As DRILL-4083 points out, when columnCount == 0, cpuCost becomes zero,
    // which makes the costs of HiveScan and HiveNativeParquetScan the same
    double cpuCost = rowCount * Math.max(columnCount, 1); // For now, assume cpu cost is proportional to row count.

    // If a positive value for CPU cost is given multiply the default CPU cost by given CPU cost.
    if (stats.getCpuCost() > 0) {
      cpuCost *= stats.getCpuCost();
    }

    // Even though scan is reading from disk, in the currently generated plans all plans will
    // need to read the same amount of data, so keeping the disk io cost 0 is ok for now.
    // In the future we might consider alternative scans that go against projections or
    // different compression schemes etc that affect the amount of data read. Such alternatives
    // would affect both cpu and io cost.
    double ioCost = 0;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(rowCount, cpuCost, ioCost, 0);
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitOldScan(this, value);
  }

  @Override
  public SelectionVectorMode[] getSupportedEncodings() {
    return SelectionVectorMode.DEFAULT;
  }

  @Override
  public SelectionVectorMode getEncoding() {
    return SelectionVectorMode.NONE;
  }

  @Override
  public boolean needsFinalColumnReordering() {
    return true;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return groupScan.getDistributionAffinity();
  }

  @Override
  public RelOptTable getTable() {
    return new DummyRelOptTable();
  }

  /**
   * minimal implementation of RelOptTable used by {@link com.dremio.sabot.op.fromjson.ConvertFromJsonConverter}
   */
  private class DummyRelOptTable implements RelOptTable {
    @Override
    public List<String> getQualifiedName() {
      return qualifiedTableName;
    }

    @Override
    public double getRowCount() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelDataType getRowType() {
      return rowType;
    }

    @Override
    public RelOptSchema getRelOptSchema() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<RelCollation> getCollationList() {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelDistribution getDistribution() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Expression getExpression(Class clazz) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RelOptTable extend(List<RelDataTypeField> extendedFields) {
      throw new UnsupportedOperationException();
    }
  }
}
