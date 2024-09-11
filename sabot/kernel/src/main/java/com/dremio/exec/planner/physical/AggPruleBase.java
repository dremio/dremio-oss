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

import com.dremio.exec.expr.fn.ItemsSketch.ItemsSketchFunctions;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.dremio.exec.planner.sql.DremioSqlOperatorTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlKind;

// abstract base class for the aggregation physical rules
public abstract class AggPruleBase extends Prule {

  protected AggPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  protected List<DistributionField> getInputDistributionField(AggregateRel rel, boolean allFields) {
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int i : rel.getGroupSet()) {
      DistributionField field = new DistributionField(i);
      groupByFields.add(field);
      if (!allFields && groupByFields.size() == 1) {
        // if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

  protected List<DistributionField> getDistributionField(AggregateRel rel, boolean allFields) {
    List<DistributionField> groupByFields = Lists.newArrayList();

    for (int i = 0; i < rel.getGroupSet().cardinality(); i++) {
      DistributionField field = new DistributionField(i);
      groupByFields.add(field);
      if (!allFields && groupByFields.size() == 1) {
        // if we are only interested in 1 grouping field, pick the first one for now..
        // but once we have num distinct values (NDV) statistics, we should pick the one
        // with highest NDV.
        break;
      }
    }

    return groupByFields;
  }

  private static final Set<String> twoPhaseFunctions =
      ImmutableSet.of(
          "SUM",
          "MIN",
          "MAX",
          "COUNT",
          "$SUM0",
          "HLL_MERGE",
          "HLL",
          "TDIGEST",
          "LISTAGG",
          ItemsSketchFunctions.FUNCTION_NAME);

  // Create 2 phase aggr plan for aggregates such as SUM, MIN, MAX
  // If any of the aggregate functions are not one of these, then we
  // currently won't generate a 2 phase plan.
  protected static boolean create2PhasePlan(RelOptRuleCall call, AggregateRel aggregate) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (!settings.isMultiPhaseAggEnabled() || isSingleton(call)) {
      return false;
    }

    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      String name = aggCall.getAggregation().getName();
      if (!twoPhaseFunctions.contains(name)) {
        return false;
      }
    }
    return true;
  }

  public static List<AggregateCall> getUpdatedPhase1AggregateCall(AggregateRel aggregateRel) {
    final RelDataTypeFactory relDataTypeFactory = aggregateRel.getCluster().getTypeFactory();
    // return rewritten phase1 aggregate calls after expansion
    return aggregateRel.getAggCallList().stream()
        .map(
            call -> {
              if (call.getAggregation().getKind() == SqlKind.LISTAGG) {
                return AggregateCall.create(
                    DremioSqlOperatorTable.LOCAL_LISTAGG,
                    call.isDistinct(),
                    call.isApproximate(),
                    call.getArgList(),
                    call.filterArg,
                    call.collation,
                    DremioSqlOperatorTable.LOCAL_LISTAGG.inferReturnType(
                        relDataTypeFactory,
                        call.getArgList().stream()
                            .map(
                                i ->
                                    aggregateRel
                                        .getInput()
                                        .getRowType()
                                        .getFieldList()
                                        .get(i)
                                        .getType())
                            .collect(Collectors.toList())),
                    call.name);
              } else {
                return call;
              }
            })
        .collect(Collectors.toList());
  }

  protected static boolean isSingleton(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    if (settings.isSingleMode()) {
      return true;
    }
    RelNode child = call.rel(0).getInputs().get(0);
    // if small input, then singleton
    return call.getMetadataQuery().getRowCount(child) < settings.getSliceTarget();
  }
}
