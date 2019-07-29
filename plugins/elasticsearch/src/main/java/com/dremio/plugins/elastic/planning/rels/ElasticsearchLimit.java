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

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dremio.exec.calcite.logical.SampleCrel;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.LimitRelBase;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.cost.DremioCost.Factory;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.BatchSchema.SelectionVectorMode;
import com.dremio.plugins.elastic.planning.rules.ElasticSampleRule;

public class ElasticsearchLimit extends LimitRelBase implements ElasticsearchPrel, ElasticTerminalPrel {

  private static final Logger logger = LoggerFactory.getLogger(ElasticsearchFilter.class);

  private final int fetchSize;
  private final int offsetSize;
  private final StoragePluginId pluginId;

  public ElasticsearchLimit(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode offset, RexNode fetch, boolean pushDown, StoragePluginId pluginId) {
    super(cluster, traits, child, offset, fetch, pushDown);
    fetchSize = getFetch() == null ? 0 : Math.max(0, RexLiteral.intValue(getFetch()));
    offsetSize = getOffset() == null ? 0 : Math.max(0, RexLiteral.intValue(getOffset()));
    this.pluginId = pluginId;
    assert offsetSize == 0; // currently do not support offset
  }

  public StoragePluginId getPluginId() {
    return pluginId;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    if(PrelUtil.getSettings(getCluster()).useDefaultCosting()) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    double cpuCost = DremioCost.COMPARE_CPU_COST * fetchSize;
    Factory costFactory = (Factory)planner.getCostFactory();
    return costFactory.makeCost(fetchSize, cpuCost, 0, 0);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new ElasticsearchLimit(getCluster(), traitSet, sole(inputs), offset, fetch, isPushDown(), pluginId);
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Merges this limit with an ElasticsearchSample operator.
   * @param sample
   * @param treeWithoutLimit
   * @return
   */
  public ElasticsearchLimit merge(ElasticsearchSample sample, RelNode treeWithoutLimit){
    if(sample == null){
      return this;
    }

    long sampleSize = SampleCrel.getSampleSizeAndSetMinSampleSize(PrelUtil.getPlannerSettings(getCluster().getPlanner()), ElasticSampleRule.SAMPLE_SIZE_DENOMINATOR);
    int limitAmount = RexLiteral.intValue(getFetch());
    int finalLimit = Math.min((int) sampleSize,  limitAmount);
    RexNode offset = getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(0), getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
    RexNode fetch = getCluster().getRexBuilder().makeExactLiteral(BigDecimal.valueOf(finalLimit), getCluster().getTypeFactory().createSqlType(SqlTypeName.INTEGER));
    return new ElasticsearchLimit(getCluster(), treeWithoutLimit.getTraitSet(), treeWithoutLimit, offset, fetch, isPushDown(), getPluginId());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> prelVisitor, X value) throws E {
    return prelVisitor.visitPrel(this, value);
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
