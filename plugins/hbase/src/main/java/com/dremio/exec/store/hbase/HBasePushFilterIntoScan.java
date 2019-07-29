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

package com.dremio.exec.store.hbase;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.planner.logical.ParseContext;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.RexToExpr;
import com.dremio.exec.planner.physical.FilterPrel;
import com.dremio.exec.planner.physical.PrelUtil;
import com.dremio.exec.planner.physical.ProjectPrel;
import com.dremio.exec.store.TableMetadata;
import com.dremio.service.namespace.NamespaceException;
import com.dremio.service.namespace.PartitionChunkMetadata;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public abstract class HBasePushFilterIntoScan extends RelOptRule {

  private HBasePushFilterIntoScan(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  public static final RelOptRule FILTER_ON_SCAN = new HBasePushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(HBaseScanPrel.class)), "HBasePushFilterIntoScan:Filter_On_Scan") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      final HBaseScanPrel scan = call.rel(1);
      final RexNode condition = filter.getCondition();

      /*
       * The rule can get triggered again due to the transformed "scan => filter" sequence
       * created by the earlier execution of this rule when we could not do a complete
       * conversion of Calcite Filter's condition to HBase Filter. In such cases, we rely upon
       * this flag to not do a re-processing of the rule on the already transformed call.
       */
      if(scan.hasFilter()) {
        return;
      }

      doPushFilterToScan(call, filter, null, scan, condition);
    }

  };


  public static final RelOptRule FILTER_ON_PROJECT = new HBasePushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(HBaseScanPrel.class))), "HBasePushFilterIntoScan:Filter_On_Project") {

    @Override
    public void onMatch(RelOptRuleCall call) {
      final FilterPrel filter = call.rel(0);
      final ProjectPrel project = call.rel(1);
      final HBaseScanPrel scan = call.rel(2);

      /*
       * The rule can get triggered again due to the transformed "scan => filter" sequence
       * created by the earlier execution of this rule when we could not do a complete
       * conversion of Calcite Filter's condition to HBase Filter. In such cases, we rely upon
       * this flag to not do a re-processing of the rule on the already transformed call.
       */
      if(scan.hasFilter()) {
        return;
      }

      // convert the filter to one that references the child of the project
      final RexNode condition =  RelOptUtil.pushPastProject(filter.getCondition(), project);

      doPushFilterToScan(call, filter, project, scan, condition);
    }

  };


  protected void doPushFilterToScan(final RelOptRuleCall call, final FilterPrel filter, final ProjectPrel project, final HBaseScanPrel scan, final RexNode condition) {

    final LogicalExpression conditionExp = RexToExpr.toExpr(new ParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan.getRowType(), scan.getCluster().getRexBuilder(), condition);
    final HBaseFilterBuilder hbaseFilterBuilder = new HBaseFilterBuilder(TableNameGetter.getTableName(scan.getTableMetadata().getName()), scan.getStartRow(), scan.getStopRow(), scan.getFilter(), conditionExp);
    final HBaseScanSpec newScanSpec = hbaseFilterBuilder.parseTree();
    if (newScanSpec == null) {
      return; //no filter pushdown ==> No transformation.
    }

    Predicate<PartitionChunkMetadata> predicate = newScanSpec.getRowKeyPredicate();

    TableMetadata metadata = scan.getTableMetadata();
    if(predicate != null) {
      try {
        metadata = metadata.prune(predicate);
      } catch (NamespaceException ex) {
        throw Throwables.propagate(ex);
      }
    }

    final HBaseScanPrel newScanPrel = new HBaseScanPrel(scan.getCluster(), scan.getTraitSet(), scan.getTable(), metadata, scan.getProjectedColumns(), scan.getObservedRowcountAdjustment(), newScanSpec.getStartRow(), newScanSpec.getStopRow(), newScanSpec.getSerializedFilter());

    // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
    final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode)newScanPrel));;

    if (hbaseFilterBuilder.isAllExpressionsConverted()) {
        /*
         * Since we could convert the entire filter condition expression into an HBase filter,
         * we can eliminate the filter operator altogether.
         */
      call.transformTo(childRel);
    } else {
      call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
    }
  }


}
