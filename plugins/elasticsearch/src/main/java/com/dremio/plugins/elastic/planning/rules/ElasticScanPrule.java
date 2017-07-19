/*
 * Copyright 2016 Dremio Corporation
 */
package com.dremio.plugins.elastic.planning.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchScanDrel;
import com.dremio.plugins.elastic.planning.rels.ElasticIntermediateScanPrel;

/**
 * Rule that converts elastic logical to physical scan
 */
public class ElasticScanPrule extends RelOptRule {

  private final FunctionLookupContext lookupContext;

  public ElasticScanPrule(FunctionLookupContext lookupContext) {
    super(RelOptHelper.any(ElasticsearchScanDrel.class), "ElasticScanPrule");
    this.lookupContext = lookupContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    ElasticsearchScanDrel logicalScan = call.rel(0);
    ElasticIntermediateScanPrel physicalScan = new ElasticIntermediateScanPrel(
        logicalScan.getCluster(),
        logicalScan.getTraitSet().replace(Prel.PHYSICAL),
        logicalScan.getTable(),
        logicalScan.getTableMetadata(),
        logicalScan.getProjectedColumns(),
        logicalScan.getObservedRowcountAdjustment()
        );

    RelNode converted = new ElasticsearchIntermediatePrel(
        physicalScan.getTraitSet(),
        physicalScan,
        lookupContext);

    call.transformTo(converted);
  }
}
