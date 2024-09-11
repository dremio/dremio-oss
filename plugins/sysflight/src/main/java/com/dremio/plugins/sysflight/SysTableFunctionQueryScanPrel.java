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
package com.dremio.plugins.sysflight;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.jetbrains.annotations.NotNull;

public class SysTableFunctionQueryScanPrel extends SysTableFunctionQueryRelBase
    implements LeafPrel {
  private final int executorCount;
  private final ImmutableList<SchemaPath> projectedColumns;

  public SysTableFunctionQueryScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelDataType rowType,
      SysTableFunctionCatalogMetadata metadata,
      String user) {
    super(cluster, traitSet, rowType, metadata, user);
    this.executorCount = PrelUtil.getPlannerSettings(cluster).getExecutorCount();
    this.projectedColumns = getAllColumns();
  }

  private ImmutableList<SchemaPath> getAllColumns() {
    return FluentIterable.from(getRowType().getFieldNames())
        .transform(input -> SchemaPath.getSimplePath(input))
        .toList();
  }

  @Override
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
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
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new SysFlightGroupScan(
        creator.props(this, getUser(), getMetadata().getBatchSchema(), null, null),
        getMetadata().getFunctionPath(),
        getMetadata().getBatchSchema(),
        projectedColumns,
        null,
        getMetadata().getStoragePluginId(),
        false,
        executorCount,
        getMetadata());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value)
      throws E {
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
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SysTableFunctionQueryScanPrel(
        getCluster(), getTraitSet(), getRowType(), getMetadata(), getUser());
  }

  @NotNull
  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }
}
