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
package com.dremio.exec.calcite.logical;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.acceleration.IncrementallyUpdateable;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.google.common.base.Predicate;

public class ScanCrel extends ScanRelBase implements CopyToCluster, IncrementallyUpdateable {

  private final boolean isDirectNamespaceDescendent;

  public ScanCrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      StoragePluginId pluginId,
      TableMetadata metadata,
      List<SchemaPath> projectedColumns,
      double observedRowcountAdjustment,
      boolean isDirectNamespaceDescendent) {
    super(cluster, traitSet, new RelOptNamespaceTable(metadata, cluster), pluginId, metadata, projectedColumns, observedRowcountAdjustment);
    this.isDirectNamespaceDescendent = isDirectNamespaceDescendent;
  }

  @Override
  public RelOptCost computeSelfCost(final RelOptPlanner planner, final RelMetadataQuery mq) {
    // Check if the data source requires ScanCrels to be converted to other node types.
    // This is enforced by making the cost infinite.
    if (pluginId.getCapabilities().getCapability(SourceCapabilities.TREAT_CALCITE_SCAN_COST_AS_INFINITE)) {
      return planner.getCostFactory().makeInfiniteCost();
    }
    return super.computeSelfCost(planner, mq);
  }

  @Override
  public RelNode copyWith(CopyWithCluster copier) {
    return new ScanCrel(
      copier.getCluster(),
      copier.copyOf(getTraitSet()),
      getPluginId(),
      getTableMetadata(),
      getProjectedColumns(),
      observedRowcountAdjustment,
      isDirectNamespaceDescendent
    );
  }

  public ScanCrel applyRowDiscount(double additionalAdjustment) {
    return new ScanCrel(getCluster(), traitSet, pluginId, getTableMetadata(), getProjectedColumns(), observedRowcountAdjustment * additionalAdjustment, isDirectNamespaceDescendent);
  }

  @Override
  public TableScan projectInvisibleColumn(String name) {
    // Check if already included
    if (getProjectedColumns().stream().map(path -> path.getRootSegment().getPath())
        .anyMatch(java.util.function.Predicate.isEqual(name))) {
      // already included.
      return this;
    }

    // Check if present in table schema
    try {
      if (getTableMetadata().getSchema().findField(name) == null) {
        // safe-guarding. Method is supposed to throw if field is not found
        return null;
      }
    } catch (IllegalArgumentException e) {
      // field not found
      return null;
    }

    List<SchemaPath> columns = new ArrayList<>();
    columns.addAll(getProjectedColumns());
    columns.add(SchemaPath.getSimplePath(name));
    return this.cloneWithProject(columns);
  }

  @Override
  public ScanCrel filterColumns(final Predicate<String> predicate) {
    List<SchemaPath> newProjection = getProjectedColumns().stream().filter(input -> {
      String path = input.getRootSegment().getNameSegment().getPath();
      return predicate.apply(path);
    }).collect(Collectors.toList());

    return new ScanCrel(getCluster(), getTraitSet(), getPluginId(), getTableMetadata(), newProjection, observedRowcountAdjustment, false);
  }

  @Override
  public ScanCrel cloneWithProject(List<SchemaPath> projection) {
    return new ScanCrel(getCluster(), traitSet, pluginId, tableMetadata, projection, observedRowcountAdjustment, false);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanCrel(getCluster(), traitSet, pluginId, tableMetadata, getProjectedColumns(), observedRowcountAdjustment, isDirectNamespaceDescendent);
  }

  public boolean isDirectNamespaceDescendent() {
    return isDirectNamespaceDescendent;
  }

}
