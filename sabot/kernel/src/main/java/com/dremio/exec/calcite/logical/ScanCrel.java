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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.CopyWithCluster;
import org.apache.calcite.plan.CopyWithCluster.CopyToCluster;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.planner.acceleration.IncrementallyUpdateable;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.store.RelOptNamespaceTable;
import com.dremio.service.namespace.StoragePluginId;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

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

  @Override
  public boolean equals(final Object other) {
    if (!(other instanceof ScanCrel)) {
      return false;
    }
    ScanCrel otherScan = (ScanCrel) other;

    // for acceleration purposes, no need to do deep comparison.
    if(otherScan.isDirectNamespaceDescendent && isDirectNamespaceDescendent){
      return getTableMetadata().getName().equals(otherScan.getTableMetadata().getName());
    }
    return super.equals(other);
  }

  @Override
  public int hashCode() {
    if(isDirectNamespaceDescendent){
      return getTableMetadata().getName().hashCode();
    }

    return super.hashCode();
  }


  @Override
  public TableScan projectInvisibleColumn(String name) {
    Set<String> names = new HashSet<>();
    names.addAll(FluentIterable.from(projectedColumns).transform(new Function<SchemaPath, String>(){

      @Override
      public String apply(SchemaPath input) {
        return input.getRootSegment().getPath();
      }}).toList());

    if(names.contains(name)){
      // already included.
      return this;
    }
    List<SchemaPath> columns = new ArrayList<>();
    columns.addAll(projectedColumns);
    columns.add(SchemaPath.getSimplePath(name));
    return this.cloneWithProject(columns);
  }

  @Override
  public ScanCrel filterColumns(final Predicate<String> predicate) {
    List<SchemaPath> newProjection = FluentIterable.from(getProjectedColumns()).filter(new Predicate<SchemaPath>(){

      @Override
      public boolean apply(SchemaPath input) {
        String path = input.getRootSegment().getNameSegment().getPath();
        return predicate.apply(path);
      }}).toList();

    return new ScanCrel(getCluster(), getTraitSet(), getPluginId(), getTableMetadata(), newProjection, observedRowcountAdjustment, false);
  }

  @Override
  public ScanCrel cloneWithProject(List<SchemaPath> projection) {
    return new ScanCrel(getCluster(), traitSet, pluginId, tableMetadata, projection, observedRowcountAdjustment, false);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ScanCrel(getCluster(), traitSet, pluginId, tableMetadata, projectedColumns, observedRowcountAdjustment, isDirectNamespaceDescendent);
  }


}
