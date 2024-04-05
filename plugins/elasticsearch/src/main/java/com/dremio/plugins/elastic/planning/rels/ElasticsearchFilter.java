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
import com.dremio.exec.planner.common.FilterRelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import java.io.IOException;
import java.util.Iterator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticsearchFilter extends FilterRelBase implements ElasticsearchPrel {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFilter.class);

  private StoragePluginId pluginId;

  public ElasticsearchFilter(
      RelOptCluster cluster,
      RelTraitSet traits,
      RelNode child,
      RexNode condition,
      StoragePluginId pluginId) {
    super(Prel.PHYSICAL, cluster, traits, child, condition);
    this.pluginId = pluginId;
  }

  @Override
  public boolean canHaveContains() {
    return true;
  }

  @Override
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @Override
  public ElasticsearchFilter copy(RelTraitSet relTraitSet, RelNode relNode, RexNode rexNode) {
    return new ElasticsearchFilter(getCluster(), relTraitSet, relNode, rexNode, pluginId);
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return super.computeSelfCost(planner, mq).multiplyBy(0.1D);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator physicalPlanCreator)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> prelVisitor, X x) throws E {
    return prelVisitor.visitPrel(this, x);
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
    return PrelUtil.iter(getInput());
  }

  @Override
  public BatchSchema getSchema(FunctionLookupContext context) {
    final ElasticsearchPrel child = (ElasticsearchPrel) getInput();
    return child.getSchema(context);
  }

  @Override
  public ScanBuilder newScanBuilder() {
    return new ScanBuilder();
  }
}
