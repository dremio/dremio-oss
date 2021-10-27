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
package com.dremio.plugins.sysflight;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.google.common.collect.ImmutableList;

/**
 * Implements filter pushdown (if filters are present) of all the filters and a fall back mechanism in case the
 * required columns are not indexed in the service exposing the System Table
 */

public abstract class SysFlightPushFilterIntoScan extends RelOptRule {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(SysFlightPushFilterIntoScan.class);

  public static final RelOptRule IS_FILTER_ON_PROJECT =
    new SysFlightPushFilterIntoScan(
      RelOptHelper.some(FilterPrel.class,
        RelOptHelper.some(ProjectPrel.class,
          RelOptHelper.any(SysFlightScanPrel.class))),
      "SysFlightPushFilterIntoScan:Filter_On_Project") {

      @Override
      public boolean matches(RelOptRuleCall call) {
        final SysFlightScanPrel scan = (SysFlightScanPrel) call.rel(2);
        return !scan.hasFilter();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filterRel = call.rel(0);
        final ProjectPrel projectRel = call.rel(1);
        final SysFlightScanPrel scanRel = call.rel(2);
        doMatch(call, scanRel, projectRel, filterRel);
      }
    };

  public static final RelOptRule IS_FILTER_ON_SCAN =
    new SysFlightPushFilterIntoScan(
      RelOptHelper.some(FilterPrel.class,
        RelOptHelper.any(SysFlightScanPrel.class)),
      "SysFlightPushFilterIntoScan:Filter_On_Scan") {

      @Override
      public boolean matches(RelOptRuleCall call) {
        final SysFlightScanPrel scan = (SysFlightScanPrel) call.rel(1);
        return !scan.hasFilter();
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
        final FilterPrel filterRel = call.rel(0);
        final SysFlightScanPrel scanRel = call.rel(1);
        doMatch(call, scanRel, null, filterRel);
      }
    };

  private SysFlightPushFilterIntoScan(RelOptRuleOperand operand, String id) {
    super(operand, id);
  }

  protected void doMatch(RelOptRuleCall call, SysFlightScanPrel scan, ProjectPrel project, FilterPrel filter) {
    if(scan.hasFilter()) {
      return;
    }

    ExpressionConverter.PushdownResult result = ExpressionConverter.pushdown(scan.getCluster().getRexBuilder(), scan.getRowType(), filter.getCondition());

    if(result.getQuery() == null) {
      LOGGER.debug("Filter didn't get pushed down because it had unsupported operator(s)");
      return; //no filter pushdown ==> No transformation.
    }

    RelNode input = new SysFlightScanPrel(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getTableMetadata(), result.getQuery(), scan.getProjectedColumns(), scan.getObservedRowcountAdjustment());

    if (project != null) {
      input = project.copy(project.getTraitSet(), input, project.getProjects(), filter.getRowType());
    }
    call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(input)));

  }

}
