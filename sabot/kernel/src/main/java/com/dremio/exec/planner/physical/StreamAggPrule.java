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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationImpl;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.AggPrelBase.OperatorPhase;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class StreamAggPrule extends AggPruleBase {
  public static final RelOptRule INSTANCE = new StreamAggPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private StreamAggPrule() {
    super(RelOptHelper.some(AggregateRel.class, RelOptHelper.any(RelNode.class)), "StreamAggPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return PrelUtil.getPlannerSettings(call.getPlanner()).isStreamAggEnabled();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final AggregateRel aggregate = (AggregateRel) call.rel(0);
    RelNode input = aggregate.getInput();
    final RelCollation collation = getCollation(aggregate);
    RelTraitSet traits = null;

    if (aggregate.containsDistinctCall()) {
      // currently, don't use StreamingAggregate if any of the logical aggrs contains DISTINCT
      return;
    }

    try {
      if (aggregate.getGroupSet().isEmpty()) {
        DistributionTrait singleDist = DistributionTrait.SINGLETON;
        final RelTraitSet singleDistTrait = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(singleDist);

        if (create2PhasePlan(call, aggregate)) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL) ;

          RelNode convertedInput = convert(input, traits);
          new SubsetTransformer<AggregateRel, InvalidRelException>(call){

            @Override
            public RelNode convertChild(final AggregateRel join, final RelNode rel) throws InvalidRelException {
              DistributionTrait toDist = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
              RelTraitSet traits = newTraitSet(Prel.PHYSICAL, toDist);
              RelNode newInput = convert(rel, traits);

              StreamAggPrel phase1Agg = new StreamAggPrel(
                  aggregate.getCluster(),
                  traits,
                  newInput,
                  aggregate.indicator,
                  aggregate.getGroupSet(),
                  aggregate.getGroupSets(),
                  aggregate.getAggCallList(),
                  OperatorPhase.PHASE_1of2);

              UnionExchangePrel exch =
                  new UnionExchangePrel(phase1Agg.getCluster(), singleDistTrait, phase1Agg);

              return  new StreamAggPrel(
                  aggregate.getCluster(),
                  singleDistTrait,
                  exch,
                  aggregate.indicator,
                  phase1Agg.getPhase2GroupSet(),
                  null,
                  phase1Agg.getPhase2AggCalls(),
                  OperatorPhase.PHASE_2of2);
            }
          }.go(aggregate, convertedInput);

        } else {
          createTransformRequest(call, aggregate, input, singleDistTrait);
        }
      } else {
        // hash distribute on all grouping keys
        final DistributionTrait distOnAllKeys =
            new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
                                       ImmutableList.copyOf(getDistributionField(aggregate, true)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(collation).plus(distOnAllKeys);
        createTransformRequest(call, aggregate, input, traits);

        // Temporarily commenting out the single distkey plan since a few tpch queries (e.g 01.sql) get stuck
        // in VolcanoPlanner.canonize() method. Note that the corresponding single distkey plan for HashAggr works
        // ok.  One possibility is that here we have dist on single key but collation on all keys, so that
        // might be causing some problem.
        /// TODO: re-enable this plan after resolving the issue.
        // createTransformRequest(call, aggregate, input, traits);

        if (create2PhasePlan(call, aggregate)) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL) ;
          RelNode convertedInput = convert(input, traits);

          new SubsetTransformer<AggregateRel, InvalidRelException>(call){

            @Override
            public RelNode convertChild(final AggregateRel aggregate, final RelNode rel) throws InvalidRelException {
              DistributionTrait toDist = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
              RelTraitSet traits = newTraitSet(Prel.PHYSICAL, collation, toDist);
              RelNode newInput = convert(rel, traits);

              StreamAggPrel phase1Agg = new StreamAggPrel(
                  aggregate.getCluster(),
                  traits,
                  newInput,
                  aggregate.indicator,
                  aggregate.getGroupSet(),
                  aggregate.getGroupSets(),
                  aggregate.getAggCallList(),
                  OperatorPhase.PHASE_1of2);

              int numEndPoints = PrelUtil.getSettings(phase1Agg.getCluster()).numEndPoints();

              HashToMergeExchangePrel exch =
                  new HashToMergeExchangePrel(phase1Agg.getCluster(), phase1Agg.getTraitSet().plus(Prel.PHYSICAL).plus(distOnAllKeys),
                      phase1Agg, ImmutableList.copyOf(getDistributionField(aggregate, true)),
                      collation,
                      numEndPoints);

              return new StreamAggPrel(
                  aggregate.getCluster(),
                  exch.getTraitSet(),
                  exch,
                  aggregate.indicator,
                  phase1Agg.getPhase2GroupSet(),
                  null,
                  phase1Agg.getPhase2AggCalls(),
                  OperatorPhase.PHASE_2of2);
            }
          }.go(aggregate, convertedInput);
        } else if (isSingleton(call)){
          DistributionTrait singleDist = DistributionTrait.SINGLETON;
          final RelTraitSet singleDistTrait = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL)
            .plus(singleDist).plus(collation);
          createTransformRequest(call, aggregate, input, singleDistTrait);
        }
      }
    } catch (InvalidRelException e) {
      tracer.warn(e.toString());
    }
  }

  private void createTransformRequest(RelOptRuleCall call, AggregateRel aggregate,
                                      RelNode input, RelTraitSet traits) throws InvalidRelException {

    final RelNode convertedInput = convert(input, traits);

    StreamAggPrel newAgg = new StreamAggPrel(
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


  private RelCollation getCollation(AggregateRel rel){

    List<RelFieldCollation> fields = Lists.newArrayList();
    for (int group = 0; group < rel.getGroupSet().cardinality(); group++) {
      fields.add(new RelFieldCollation(group));
    }
    return RelCollationImpl.of(fields);
  }
}
