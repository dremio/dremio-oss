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
import java.util.List;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.AggPrelBase.OperatorPhase;
import com.dremio.exec.planner.physical.AggPrelBase.SqlHllAggFunction;
import com.google.common.collect.ImmutableList;

public class HashAggPrule extends AggPruleBase {
  public static final RelOptRule INSTANCE = new HashAggPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private HashAggPrule() {
    super(RelOptHelper.some(AggregateRel.class, RelOptHelper.any(RelNode.class)), "HashAggPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
    return settings.isMemoryEstimationEnabled() || settings.isHashAggEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (!PrelUtil.getPlannerSettings(call.getPlanner()).isHashAggEnabled()) {
      return;
    }

    final AggregateRel aggregate = (AggregateRel) call.rel(0);
    final RelNode input = call.rel(1);

    if (aggregate.containsDistinctCall() || aggregate.getGroupCount() == 0) {
      // currently, don't use HashAggregate if any of the logical aggrs contains DISTINCT or
      // if there are no grouping keys
      return;
    }

    try {
      DistributionTrait singleDist = DistributionTrait.SINGLETON;
      RelTraitSet singletonTraits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(singleDist);
      if (aggregate.getGroupSet().isEmpty()) {
        createTransformRequest(call, aggregate, input, singletonTraits);
      } else {
        // hash distribute on all grouping keys
        DistributionTrait distOnAllKeys =
            new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
                                       ImmutableList.copyOf(getDistributionField(aggregate, true /* get all grouping keys */)));

        RelTraitSet traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distOnAllKeys);
        createTransformRequest(call, aggregate, input, traits);

        // hash distribute on single grouping key
        DistributionTrait distOnOneKey =
            new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
                                       ImmutableList.copyOf(getDistributionField(aggregate, false /* get single grouping key */)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(distOnOneKey);
        createTransformRequest(call, aggregate, input, traits);

        if (create2PhasePlan(call, aggregate)) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL) ;

          RelNode convertedInput = convert(input, traits);
          new TwoPhaseSubset(call, distOnAllKeys).go(aggregate, convertedInput);

        } else {
          createTransformRequest(call, aggregate, input, singletonTraits);
        }
      }
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

  private class TwoPhaseSubset extends SubsetTransformer<AggregateRel, InvalidRelException> {
    final RelTrait distOnAllKeys;

    public TwoPhaseSubset(RelOptRuleCall call, RelTrait distOnAllKeys) {
      super(call);
      this.distOnAllKeys = distOnAllKeys;
    }

    @Override
    public RelNode convertChild(AggregateRel aggregate, RelNode input) throws InvalidRelException {

      RelTraitSet traits = newTraitSet(Prel.PHYSICAL, input.getTraitSet().getTrait(DistributionTraitDef.INSTANCE));
      RelNode newInput = convert(input, traits);

      HashAggPrel phase1Agg = new HashAggPrel(
          aggregate.getCluster(),
          traits,
          newInput,
          aggregate.indicator,
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          convertAggCallList(aggregate, aggregate.getAggCallList()),
          OperatorPhase.PHASE_1of2);

      HashToRandomExchangePrel exch =
          new HashToRandomExchangePrel(phase1Agg.getCluster(), phase1Agg.getTraitSet().plus(Prel.PHYSICAL).plus(distOnAllKeys),
              phase1Agg, ImmutableList.copyOf(getDistributionField(aggregate, true)));

      HashAggPrel phase2Agg =  new HashAggPrel(
          aggregate.getCluster(),
          exch.getTraitSet(),
          exch,
          aggregate.indicator,
          phase1Agg.getPhase2GroupSet(),
          null,
          phase1Agg.getPhase2AggCalls(),
          OperatorPhase.PHASE_2of2);
      return phase2Agg;
    }
  }

  private void createTransformRequest(RelOptRuleCall call, AggregateRel aggregate,
                                      RelNode input, RelTraitSet traits) throws InvalidRelException {

    final RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, traits));

    HashAggPrel newAgg = new HashAggPrel(
        aggregate.getCluster(),
        traits,
        convertedInput,
        aggregate.indicator,
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        OperatorPhase.PHASE_1of1);

    call.transformTo(newAgg);
  }

}
