/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rels;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.dremio.exec.planner.physical.CustomPrel;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.exec.planner.physical.visitor.PrelVisitor;
import com.dremio.exec.record.BatchSchema;

/*
 * Represents a finalized Elastic scan after a query has been generated. At this point, no further pushdowns
 * can be done. Contains the original ElasticPrel tree so that we can continue to do operations like debug
 * inspection and RelMdOrigins determination. Beyond that, three tree should not be used. (For example,
 * it won't show up when doing EXPLAIN).
 */
public class ElasticScanPrel extends AbstractRelNode implements Prel, CustomPrel {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ElasticScanPrel.class);

  private final ElasticsearchPrel input;
  private final ScanBuilder scanBuilder;

  public ElasticScanPrel(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      ElasticsearchPrel input,
      ScanBuilder scanBuilder,
      FunctionLookupContext functionLookupContext) {
    super(cluster, traitSet);
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
    return input.estimateRowCount(mq);
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("resource", scanBuilder.getResource()).item("columns", scanBuilder.getColumns()).item("pushdown\n ", scanBuilder.getQuery());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return creator.addMetadata(this, scanBuilder.toGroupScan((long) input.estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery())));
  }

  @Override
  public <T, X, E extends Throwable> T accept(PrelVisitor<T, X, E> logicalVisitor, X value) throws E {
    return logicalVisitor.visitPrel(this, value);
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

}
