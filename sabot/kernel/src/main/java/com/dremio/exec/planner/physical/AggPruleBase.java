/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.BitSets;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import com.dremio.exec.planner.cost.DefaultRelMetadataProvider;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.physical.AggPrelBase.SqlHllAggFunction;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

// abstract base class for the aggregation physical rules
public abstract class AggPruleBase extends Prule {

  protected AggPruleBase(RelOptRuleOperand operand, String description) {
    super(operand, description);
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

  private static final Set<String> twoPhaseFunctions = ImmutableSet.<String>builder().add("SUM")
    .add("MIN")
    .add("MAX")
    .add("COUNT")
    .add("$SUM0")
    .add("NDV")
    .build();

  // Create 2 phase aggr plan for aggregates such as SUM, MIN, MAX
  // If any of the aggregate functions are not one of these, then we
  // currently won't generate a 2 phase plan.
  protected boolean create2PhasePlan(RelOptRuleCall call, AggregateRel aggregate) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    RelNode child = call.rel(0).getInputs().get(0);
    boolean smallInput = child.estimateRowCount(DefaultRelMetadataProvider.INSTANCE.getRelMetadataQuery()) < settings.getSliceTarget();
    if (! settings.isMultiPhaseAggEnabled() || settings.isSingleMode() || smallInput) {
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

  protected List<AggregateCall> convertAggCallList(AggregateRel aggregate, List<AggregateCall> aggCalls) {
    List<AggregateCall> convertedCalls = new ArrayList<>();
    for (Ord<AggregateCall> aggCall : Ord.zip(aggCalls)) {
      AggregateCall newCall;
      if ("NDV".equals(aggCall.e.getAggregation().getName())) {
        newCall = AggregateCall.create(
          new SqlHllAggFunction(),
          aggCall.e.isDistinct(),
          aggCall.e.getArgList(),
          -1,
          aggregate.getCluster().getTypeFactory().createSqlType(SqlTypeName.BINARY),
          aggCall.e.getName());
      } else {
        newCall = aggCall.e;
      }
      convertedCalls.add(newCall);
    }
    return convertedCalls;
  }
}
