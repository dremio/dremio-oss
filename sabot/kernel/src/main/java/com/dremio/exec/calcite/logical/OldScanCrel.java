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
package com.dremio.exec.calcite.logical;

import java.util.List;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory.FieldInfoBuilder;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.dremio.common.expression.CompleteType;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.acceleration.IncrementallyUpdateable;
import com.dremio.exec.planner.common.OldScanRelBase;
import com.dremio.exec.planner.logical.EmptyRel;
import com.dremio.exec.planner.sql.MaterializationDescriptor.LayoutInfo;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

/**
 * Dummy scan rel until we get to convert to Dremio logical rel.  Although we don't do much with the ScanCrel,
 * we need all the costing to be consistent with ScanRel, so use the same costing model as
 * ScanRel here by extending ScanRelBase.
 */
public class OldScanCrel extends OldScanRelBase implements CopyToCluster, IncrementallyUpdateable {

  private final LayoutInfo layoutInfo;

  public OldScanCrel(
      RelOptCluster cluster,
      RelOptTable relOptTable,
      RelTraitSet traits,
      RelDataType rowType,
      GroupScan groupScan,
      LayoutInfo layoutInfo,
      double rowCountDiscount) {
    super(cluster, traits, relOptTable, rowType, groupScan, rowCountDiscount);
    this.layoutInfo = layoutInfo;
    Preconditions.checkArgument(traits.getTrait(ConventionTraitDef.INSTANCE) == Convention.NONE);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OldScanCrel that = (OldScanCrel) o;
    return Objects.equal(rowType, that.rowType) &&
        Objects.equal(groupScan, that.groupScan);
  }


  @Override
  public TableScan projectInvisibleColumn(String name) {
    RelDataTypeField addedField = getRowType().getField(name, false, false);
    if(addedField != null){
      return this;
    }

    final FieldInfoBuilder infoBuilder = new FieldInfoBuilder(getCluster().getTypeFactory());
    infoBuilder.addAll(getRowType().getFieldList());

    // hardcoded since our only invisible column is bigint. This hack will go away once we remove OldScanCrel
    final RelDataType type = CompleteType.BIGINT.toCalciteType(getCluster().getTypeFactory());
    infoBuilder.add(name, type);

    return new OldScanCrel(
      getCluster(),
      getTable(),
      getTraitSet(),
      infoBuilder.build(),
      getGroupScan(),
      layoutInfo,
      getRowCountDiscount());
  }

  @Override
  public OldScanCrel filterColumns(final Predicate<String> predicate) {
    List<SchemaPath> newColumns = FluentIterable.from(getGroupScan().getColumns()).filter(new Predicate<SchemaPath>(){
      @Override
      public boolean apply(SchemaPath input) {
        return predicate.apply(input.getRootSegment().getPath());
      }}).toList();
    final FieldInfoBuilder infoBuilder = new FieldInfoBuilder(getCluster().getTypeFactory());
    infoBuilder.addAll(FluentIterable.from(getRowType().getFieldList()).filter(new Predicate<RelDataTypeField>(){

      @Override
      public boolean apply(RelDataTypeField input) {
        return predicate.apply(input.getName());
      }}));

    return new OldScanCrel(
        getCluster(),
        getTable(),
        getTraitSet(),
        infoBuilder.build(),
        getGroupScan().clone(newColumns),
        getLayoutInfo(),
        getRowCountDiscount());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(rowType, groupScan);
  }


  public LayoutInfo getLayoutInfo() {
    return layoutInfo;
  }

  /**
   * Takes the provided group scan and return the same group scan if the schema for the scan is known.  If, for whatever reason,
   * the schema is unknown (e.g. the table has no data and we fail to sample), then we return a dummy scan object.  This is to
   * allow all schemas to have a schema even if one is not defined for the current scan.
   */
  public static RelNode create(RelOptCluster cluster, RelOptTable relOptTable, RelTraitSet traits, RelDataType rowType, GroupScan groupScan, LayoutInfo layoutInfo, double discountRowCount) {
    if(groupScan.getSchema().isUnknownSchema()){
      return new EmptyRel(cluster, traits, rowType, groupScan.getSchema());
    } else {
      return new OldScanCrel(cluster, relOptTable, traits, rowType, groupScan, layoutInfo, discountRowCount);
    }
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new OldScanCrel(
      copier.getCluster(),
      copier.copyOf(getTable()),
      getTraitSet(),
      copier.copyOf(getRowType()),
      getGroupScan(),
      getLayoutInfo(),
      getRowCountDiscount()
    );
  }
}
