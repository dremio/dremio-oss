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
import java.util.Collections;
import java.util.Iterator;

import com.dremio.options.Options;
import com.dremio.options.TypeValidators;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.fragment.DistributionAffinity;
import com.dremio.exec.planner.physical.CustomPrel;
import com.dremio.exec.planner.physical.LeafPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.BasePrelVisitor;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;

/*
 * Represents a finalized Elastic scan after a query has been generated. At this point, no further pushdowns
 * can be done. Contains the original ElasticPrel tree so that we can continue to do operations like debug
 * inspection and RelMdOrigins determination. Beyond that, three tree should not be used. (For example,
 * it won't show up when doing EXPLAIN).
 */
@Options
public class ElasticScanPrel extends TableScan implements LeafPrel, CustomPrel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticScanPrel.class);

  public static final TypeValidators.LongValidator RESERVE = new TypeValidators.PositiveLongValidator("planner.op.scan.elastic.reserve_bytes", Long.MAX_VALUE, DEFAULT_RESERVE);
  public static final TypeValidators.LongValidator LIMIT = new TypeValidators.PositiveLongValidator("planner.op.scan.elastic.limit_bytes", Long.MAX_VALUE, DEFAULT_LIMIT);

  private final ElasticsearchPrel input;
  private final ScanBuilder scanBuilder;

  private LeafPrel leafPrel;

  public ElasticScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      ElasticsearchPrel input,
      ScanBuilder scanBuilder,
      FunctionLookupContext functionLookupContext) {
    super(cluster, traitSet, scanBuilder.getTable());
    this.input = input;
    this.rowType = input.getRowType();
    this.scanBuilder = scanBuilder;
  }

  @Override
  public Prel getOriginPrel() {
    return input;
  }

  @Override
  public double estimateRowCount(RelMetadataQuery mq) {
    return mq.getRowCount(input);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("resource", scanBuilder.getResource()).item("columns", scanBuilder.getColumns()).item("pushdown\n ", scanBuilder.getQuery());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    final RelMetadataQuery mq = input.getCluster().getMetadataQuery();

    final BatchSchema schema = input.getSchema(creator.getFunctionLookupContext());
    return scanBuilder.toGroupScan(
        creator.props(this, null/*todo: need to send user?*/, schema, RESERVE, LIMIT),
        mq.getRowCount(input).longValue());
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitLeaf(this, value);
  }

  @Override
  public BatchSchema.SelectionVectorMode[] getSupportedEncodings() {
    return BatchSchema.SelectionVectorMode.DEFAULT;
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
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
  public DistributionAffinity getDistributionAffinity() {
    return DistributionAffinity.SOFT;
  }

  @Override
  public int getMaxParallelizationWidth() {
    findIntermediateScanPrel();
    if (leafPrel != null) {
      return leafPrel.getMaxParallelizationWidth();
    }
    return Integer.MAX_VALUE;
  }

  @Override
  public int getMinParallelizationWidth() {
    return 1;
  }

  private void findIntermediateScanPrel() {
    if (leafPrel == null) {
      leafPrel = input.accept(new ScanPrelFinder(), null);
    }
  }

  private class ScanPrelFinder extends BasePrelVisitor<LeafPrel,Void,RuntimeException> {
    @Override
    public LeafPrel visitPrel(Prel prel, Void v) {
      LeafPrel leafPrel = null;
      for (Prel child : prel) {
        leafPrel = child.accept(this, v);
        if (leafPrel != null) {
          return leafPrel;
        }
      }
      return null;
    }

    @Override
    public LeafPrel visitLeaf(LeafPrel leafPrel, Void v) {
      return ElasticScanPrel.this.leafPrel;
    }
  }
}
