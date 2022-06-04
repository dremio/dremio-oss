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

package com.dremio.exec.store.ischema;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.store.ischema.ExpressionConverter.PushdownResult;
import com.google.common.collect.ImmutableList;

public abstract class InfoSchemaPushFilterIntoScan extends RelOptRule {

  public static final RelOptRule IS_FILTER_ON_PROJECT =
      new InfoSchemaPushFilterIntoScan(
          RelOptHelper.some(FilterPrel.class,
              RelOptHelper.some(ProjectPrel.class,
                  RelOptHelper.any(InfoSchemaScanPrel.class))),
          "InfoSchemaPushFilterIntoScan:Filter_On_Project") {

    @Override
    public boolean matches(RelOptRuleCall call) {
      final InfoSchemaScanPrel scan = (InfoSchemaScanPrel) call.rel(2);
      return !scan.hasFilter();
    }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final FilterPrel filterRel = call.rel(0);
          final ProjectPrel projectRel = call.rel(1);
          final InfoSchemaScanPrel scanRel = call.rel(2);
          doMatch(call, scanRel, projectRel, filterRel);
        }
      };

  public static final RelOptRule IS_FILTER_ON_SCAN =
      new InfoSchemaPushFilterIntoScan(
          RelOptHelper.some(FilterPrel.class,
              RelOptHelper.any(InfoSchemaScanPrel.class)),
          "InfoSchemaPushFilterIntoScan:Filter_On_Scan") {

        @Override
        public boolean matches(RelOptRuleCall call) {
          final InfoSchemaScanPrel scan = (InfoSchemaScanPrel) call.rel(1);
          return !scan.hasFilter();
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
          final FilterPrel filterRel = call.rel(0);
          final InfoSchemaScanPrel scanRel = call.rel(1);
          doMatch(call, scanRel, null, filterRel);
        }
      };

  private InfoSchemaPushFilterIntoScan(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  protected void doMatch(RelOptRuleCall call, InfoSchemaScanPrel scan, ProjectPrel project, FilterPrel filter) {
    if(scan.hasFilter()) {
      return;
    }

    PushdownResult result = ExpressionConverter.pushdown(scan.getCluster().getRexBuilder(), scan.getRowType(), filter.getCondition());

    if(result.getQuery() == null) {
      return; //no filter pushdown ==> No transformation.
    }

    RelNode input = new InfoSchemaScanPrel(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getTableMetadata(), result.getQuery(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), scan.getRuntimeFilters());

    if (project != null) {
      input = project.copy(project.getTraitSet(), input, project.getProjects(), filter.getRowType());
    }

    if (result.getRemainder() == null) {
      // Filter can be removed as all expressions in the filter are converted and pushed to scan
      call.transformTo(input);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(input)));
    }
  }
}
