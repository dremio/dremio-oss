/*
 * Copyright (C) 2017 Dremio Corporation
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

package com.dremio.exec.store.ischema;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.planner.physical.OldScanPrel;
import com.dremio.exec.store.StoragePluginOptimizerRule;
import com.google.common.collect.ImmutableList;

public abstract class InfoSchemaPushFilterIntoRecordGenerator extends StoragePluginOptimizerRule {

  public static final StoragePluginOptimizerRule IS_FILTER_ON_PROJECT =
      new InfoSchemaPushFilterIntoRecordGenerator(
          RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(OldScanPrel.class))),
          "InfoSchemaPushFilterIntoRecordGenerator:Filter_On_Project") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          final OldScanPrel scan = (OldScanPrel) call.rel(2);
          GroupScan groupScan = scan.getGroupScan();
          return groupScan instanceof InfoSchemaGroupScan;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final FilterPrel filterRel = (FilterPrel) call.rel(0);
          final ProjectPrel projectRel = (ProjectPrel) call.rel(1);
          final OldScanPrel scanRel = call.rel(2);
          doMatch(call, scanRel, projectRel, filterRel);
        }
      };

  public static final StoragePluginOptimizerRule IS_FILTER_ON_SCAN =
      new InfoSchemaPushFilterIntoRecordGenerator(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(OldScanPrel.class)),
          "InfoSchemaPushFilterIntoRecordGenerator:Filter_On_Scan") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          final OldScanPrel scan = (OldScanPrel) call.rel(1);
          GroupScan groupScan = scan.getGroupScan();
          return groupScan instanceof InfoSchemaGroupScan;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final FilterPrel filterRel = (FilterPrel) call.rel(0);
          final OldScanPrel scanRel = (OldScanPrel) call.rel(1);
          doMatch(call, scanRel, null, filterRel);
        }
      };

  private InfoSchemaPushFilterIntoRecordGenerator(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  protected void doMatch(RelOptRuleCall call, OldScanPrel scan, ProjectPrel project, FilterPrel filter) {
    final RexNode condition = filter.getCondition();

    InfoSchemaGroupScan groupScan = (InfoSchemaGroupScan)scan.getGroupScan();
    if (groupScan.isFilterPushedDown()) {
      return;
    }

    LogicalExpression conditionExp =
        RexToExpr.toExpr(new ParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), project != null ? project : scan, condition);
    InfoSchemaFilterBuilder filterBuilder = new InfoSchemaFilterBuilder(conditionExp);
    InfoSchemaFilter infoSchemaFilter = filterBuilder.build();
    if (infoSchemaFilter == null) {
      return; //no filter pushdown ==> No transformation.
    }

    final InfoSchemaGroupScan newGroupsScan = new InfoSchemaGroupScan(groupScan.getTable(), infoSchemaFilter, groupScan.getContext());
    newGroupsScan.setFilterPushedDown(true);

    RelNode input =
        OldScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType(), scan.getQualifiedTableName());
    if (project != null) {
      input = project.copy(project.getTraitSet(), input, project.getProjects(), filter.getRowType());
    }

    if (filterBuilder.isAllExpressionsConverted()) {
      // Filter can be removed as all expressions in the filter are converted and pushed to scan
      call.transformTo(input);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(input)));
    }
  }
}
