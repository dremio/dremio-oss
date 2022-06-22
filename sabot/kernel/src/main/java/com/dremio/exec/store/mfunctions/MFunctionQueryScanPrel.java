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
package com.dremio.exec.store.mfunctions;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.MFunctionCatalogMetadata;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

/**
 * Physical RelNode of metadata functions scan.
 */
final class MFunctionQueryScanPrel extends MFunctionQueryRelBase implements LeafPrel {

  private final MFunctionCatalogMetadata tableMetadata;
  private final ImmutableList<SchemaPath> projectedColumns;

  public MFunctionQueryScanPrel(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType,
                                MFunctionCatalogMetadata tableMetadata, String user, String metadataLocation) {
    super(cluster, traitSet, rowType, tableMetadata, user, metadataLocation);
    this.tableMetadata = tableMetadata;
    this.projectedColumns = getAllColumns();
  }

  /**
   * in case of another way to provide projected columns, replace this.
   */
  private ImmutableList<SchemaPath> getAllColumns(){
    return FluentIterable.from(getRowType().getFieldNames()).transform(input -> SchemaPath.getSimplePath(input)).toList();
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new MFunctionQueryScanPrel(
      getCluster(),
      getTraitSet(),
      getRowType(),
      tableMetadata,
      user,
      metadataLocation);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new MetadataFunctionsGroupScan(
      creator.props(this, user, tableMetadata.getBatchSchema()),
      tableMetadata,
      tableMetadata.getBatchSchema(),
      projectedColumns,
      metadataLocation);
  }


  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
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
    return false;
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
      .item("source", tableMetadata.getStoragePluginId().getName())
      .item("mFileType", tableMetadata.getFileType())
      .item("mFunction", tableMetadata.getMetadataFunctionName())
      .item("table", tableMetadata.getNamespaceKey());
  }

}
