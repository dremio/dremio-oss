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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.logical.TableModifyRel;
import com.dremio.exec.store.TableMetadata;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;

/** A base physical plan generator for a TableModifyRel */
public abstract class TableModifyPruleBase extends Prule {

  private final OptimizerRulesContext context;

  public TableModifyPruleBase(
      RelOptRuleOperand operand, String description, OptimizerRulesContext context) {
    super(operand, description);
    this.context = context;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    throw new UnsupportedOperationException();
  }

  public void onMatch(RelOptRuleCall call, TableMetadata tableMetadata) {
    final TableModifyRel tableModify = call.rel(0);
    final RelNode input = call.rel(1);
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());

    if (settings.options.getOption(
        ExecConstants.ENABLE_ICEBERG_MERGE_ON_READ_WRITER_WITH_POSITIONAL_DELETE)) {
      // Merge On Read Dml Case
      DmlPositionalMergeOnReadPlanGenerator planGenerator =
          new DmlPositionalMergeOnReadPlanGenerator(
              tableModify.getTable(),
              tableModify.getCluster(),
              tableModify.getTraitSet().plus(Prel.PHYSICAL),
              convert(input, input.getTraitSet().plus(Prel.PHYSICAL)),
              tableMetadata,
              tableModify.getCreateTableEntry(),
              tableModify.getOperation(),
              tableModify.getOperation() == TableModify.Operation.MERGE
                  ? tableModify.getMergeUpdateColumnList()
                  : tableModify.getUpdateColumnList(),
              tableModify.hasSource(),
              context);

      call.transformTo(planGenerator.getPlan());
    } else {
      // Copy On Write Dml Case
      DmlPlanGenerator planGenerator =
          new DmlPlanGenerator(
              tableModify.getTable(),
              tableModify.getCluster(),
              tableModify.getTraitSet().plus(Prel.PHYSICAL),
              convert(input, input.getTraitSet().plus(Prel.PHYSICAL)),
              tableMetadata,
              tableModify.getCreateTableEntry(),
              tableModify.getOperation(),
              tableModify.getOperation() == TableModify.Operation.MERGE
                  ? tableModify.getMergeUpdateColumnList()
                  : tableModify.getUpdateColumnList(),
              tableModify.hasSource(),
              context);
      call.transformTo(planGenerator.getPlan());
    }
  }
}
