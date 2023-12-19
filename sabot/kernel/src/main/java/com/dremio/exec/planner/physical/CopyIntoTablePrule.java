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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import com.dremio.exec.catalog.DremioPrepareTable;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.CopyIntoPlanBuilder;
import com.dremio.exec.planner.logical.CopyIntoTableRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.store.TableMetadata;

/**
 * A physical plan generator for 'COPY INTO'
 */
public class CopyIntoTablePrule extends RelOptRule {

  private final OptimizerRulesContext context;

  public CopyIntoTablePrule(OptimizerRulesContext context) {
    super(RelOptHelper.any(CopyIntoTableRel.class),"Prel.CopyIntoTablePrule");
    this.context = context;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final CopyIntoTableRel copyIntoTableRel = call.rel(0);
    call.transformTo(getPhysicalPlan(copyIntoTableRel, ((DremioPrepareTable) copyIntoTableRel.getTable()).getTable().getDataset()));
  }

  public Prel getPhysicalPlan(CopyIntoTableRel copyIntoTableRel, TableMetadata tableMetadata) {
    CopyIntoPlanBuilder planBuilder = new CopyIntoPlanBuilder(
      copyIntoTableRel.getTable(),
      copyIntoTableRel.getRowType(),
      copyIntoTableRel.getCluster(),
      copyIntoTableRel.getTraitSet().plus(Prel.PHYSICAL),
      tableMetadata,
      context,
      copyIntoTableRel.getContext());

    return planBuilder.buildPlan();
  }
}
