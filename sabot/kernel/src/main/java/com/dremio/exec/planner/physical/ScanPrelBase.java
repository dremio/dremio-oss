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
package com.dremio.exec.planner.physical;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.RelHint;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.physical.filter.RuntimeFilteredRel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.exec.store.ScanFilter;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.Affinity;
import com.dremio.service.namespace.dataset.proto.PartitionProtobuf.DatasetSplit;
import com.google.common.collect.ImmutableList;

public abstract class ScanPrelBase extends ScanRelBase implements LeafPrel, RuntimeFilteredRel {

  private List<Info> runtimeFilters = ImmutableList.of();

  public ScanPrelBase(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, StoragePluginId pluginId,
                      TableMetadata dataset, List<SchemaPath> projectedColumns, double observedRowcountAdjustment,
                      List<RelHint> hints, List<Info> runtimeFilters) {
    super(cluster, traitSet, table, pluginId, dataset, projectedColumns, observedRowcountAdjustment, hints);
    this.runtimeFilters = runtimeFilters;
    assert traitSet.getTrait(ConventionTraitDef.INSTANCE) != Rel.LOGICAL;
  }

  /**
   * Returns if scan has a filter pushed down into
   *
   * @return true if filter is present, false otherwise
   */
  public boolean hasFilter() {
    return false;
  }

  public ScanFilter getFilter() {
    return null;
  }

  @Override
  public int getMaxParallelizationWidth(){
    return tableMetadata.getSplitCount();
  }

  @Override
  public int getMinParallelizationWidth() {
    if(getDistributionAffinity() != DistributionAffinity.HARD){
      return 1;
    }

    final Set<String> nodes = new HashSet<>();
    Iterator<PartitionChunkMetadata> iter = tableMetadata.getSplits();
    while(iter.hasNext()){
      PartitionChunkMetadata partitionChunk = iter.next();
      for (DatasetSplit split : partitionChunk.getDatasetSplits()) {
        for (Affinity a : split.getAffinitiesList()) {
          nodes.add(a.getHost());
        }
      }
    }

    return nodes.size();
  }

  @Override
  public Iterator<Prel> iterator() {
    return Collections.emptyIterator();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitLeaf(this, value);
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
    return this.tableMetadata.getStoragePluginId().getCapabilities().getCapability(SourceCapabilities.REQUIRES_HARD_AFFINITY) ? DistributionAffinity.HARD : DistributionAffinity.SOFT;
  }

  @Override
  public double getCostForParallelization() {
    RelOptCost cost = computeSelfCost(getCluster().getPlanner(), getCluster().getMetadataQuery());
    double costForParallelization = Math.max(cost.getRows(), 1);

    PlannerSettings settings = PrelUtil.getSettings(getCluster());
    if (settings.useMinimumCostPerSplit()) {
      double minCostPerSplit = settings.getMinimumCostPerSplit(getPluginId().getType());
      costForParallelization = Math.max(costForParallelization, minCostPerSplit * tableMetadata.getSplitCount());
    }
    return costForParallelization;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    if(!runtimeFilters.isEmpty()) {
      pw.item("runtimeFilters", runtimeFilters.stream()
        .map(Object::toString)
        .collect(Collectors.joining(", ")));
    }
    return pw;
  }

  @Override
  public List<Info> getRuntimeFilters() {
    return runtimeFilters;
  }

  @Override
  public void addRuntimeFilter(Info info) {
    runtimeFilters = ImmutableList.<Info>builder()
      .addAll(runtimeFilters)
      .add(info)
      .build();
    recomputeDigest();
  }
}
