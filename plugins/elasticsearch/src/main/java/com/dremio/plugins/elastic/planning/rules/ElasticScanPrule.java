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
package com.dremio.plugins.elastic.planning.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.expr.fn.FunctionLookupContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.elastic.planning.rels.ElasticIntermediateScanPrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchIntermediatePrel;
import com.dremio.plugins.elastic.planning.rels.ElasticsearchScanDrel;
import com.google.common.collect.ImmutableList;

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
        logicalScan.getObservedRowcountAdjustment(),
        ImmutableList.of()
        );

    RelNode converted = new ElasticsearchIntermediatePrel(
        physicalScan.getTraitSet(),
        physicalScan,
        lookupContext);

    call.transformTo(converted);
  }
}
