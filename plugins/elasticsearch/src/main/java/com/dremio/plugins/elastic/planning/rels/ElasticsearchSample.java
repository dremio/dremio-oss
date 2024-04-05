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
package com.dremio.plugins.elastic.planning.rels;

import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.SampleRelBase;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class ElasticsearchSample extends SampleRelBase implements ElasticsearchPrel {

  public static final long SAMPLE_SIZE_DENOMINATOR = 10;
  private final int fetchSize;
  private final StoragePluginId pluginId;

  public ElasticsearchSample(
      RelOptCluster cluster, RelTraitSet traits, RelNode input, StoragePluginId pluginId) {
    super(cluster, traits, input);
    this.pluginId = pluginId;
    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(cluster.getPlanner());
    fetchSize =
        (int)
            SampleRelBase.getSampleSizeAndSetMinSampleSize(
                plannerSettings, SAMPLE_SIZE_DENOMINATOR);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if (PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner, mq).multiplyBy(.1);
    }

    double cpuCost = DremioCost.COMPARE_CPU_COST * fetchSize;
    Factory costFactory = (Factory) planner.getCostFactory();
    return costFactory.makeCost(fetchSize, cpuCost, 0, 0);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ElasticsearchSample(getCluster(), traitSet, sole(inputs), pluginId);
  }

  @Override
  protected Object clone() throws CloneNotSupportedException {
    return copy(getTraitSet(), getInputs());
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return fetchSize;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitPrel(this, value);
  }

  public int getFetchSize() {
    return fetchSize;
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
  public ScanBuilder newScanBuilder() {
    return new ScanBuilder();
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    final ElasticsearchPrel child = (ElasticsearchPrel) getInput();
    return child.getSchema(context);
  }

  @Override
  public StoragePluginId getPluginId() {
    return pluginId;
  }
}
