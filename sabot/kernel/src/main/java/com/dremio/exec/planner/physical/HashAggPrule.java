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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.trace.CalciteTrace;
import org.slf4j.Logger;

import com.dremio.exec.planner.common.MoreRelOptUtil;
import com.dremio.exec.planner.logical.AggregateRel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.AggregatePrel.OperatorPhase;
import com.google.common.collect.ImmutableList;

public class HashAggPrule extends AggPruleBase {
  public static final RelOptRule INSTANCE = new HashAggPrule();
  protected static final Logger tracer = CalciteTrace.getPlannerTracer();

  private HashAggPrule() {
    super(RelOptHelper.some(AggregateRel.class, RelOptHelper.any(RelNode.class)), "HashAggPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    Aggregate agg = call.rel(0);
    if (agg.groupSets == null || agg.groupSets.size() < 2) {
      PlannerSettings settings = PrelUtil.getPlannerSettings(call.getPlanner());
      return settings.isHashAggEnabled();
    }
    return false;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    if (!PrelUtil.getPlannerSettings(call.getPlanner()).isHashAggEnabled()) {
      return;
    }

    final AggregateRel aggregate = (AggregateRel) call.rel(0);
    final RelNode input = call.rel(1);

    if (MoreRelOptUtil.containsUnsupportedDistinctCall(aggregate) || (aggregate.getGroupCount() == 0 && !aggregate.containsSupportedListAggregation())) {
      // currently, don't use HashAggregate if any of the logical aggrs contains unsupported DISTINCT or
      // if there are no grouping keys. Using empty grouping key with listagg is supported.
      return;
    }

    RelTraitSet traits = null;

    try {
      if (aggregate.getGroupSet().isEmpty()) {
        DistributionTrait singleDist = DistributionTrait.SINGLETON;
        traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(singleDist);
        createTransformRequest(call, aggregate, input, traits);
      } else {
        // hash distribute on all grouping keys
        DistributionTrait inputDistOnAllKeys =
            new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
              ImmutableList.copyOf(getInputDistributionField(aggregate, true /* get all grouping keys */)));
        DistributionTrait distOnAllKeys =
          new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
            ImmutableList.copyOf(getDistributionField(aggregate, true /* get all grouping keys */)));

        traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(inputDistOnAllKeys);
        createTransformRequest(call, aggregate, input, traits);

        if (PrelUtil.getPlannerSettings(call.getPlanner()).isHashSingleKey()) {
          // hash distribute on single grouping key
          DistributionTrait inputDistOnOneKey =
            new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED,
              ImmutableList.copyOf(getInputDistributionField(aggregate, false /* get single grouping key */)));

          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(inputDistOnOneKey);
          createTransformRequest(call, aggregate, input, traits);
        }

        // if the call supports creating a two phase plan, also create and transform to the two-phase version of plan
        if (create2PhasePlan(call, aggregate)) {
          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL) ;

          RelNode convertedInput = convert(input, traits);
          new TwoPhaseSubset(call, distOnAllKeys).go(aggregate, convertedInput);
        } else if (isSingleton(call)) {
          // if the agg should be singleton, create and transform to the singleton plan
          DistributionTrait singleDist = DistributionTrait.SINGLETON;
          traits = call.getPlanner().emptyTraitSet().plus(Prel.PHYSICAL).plus(singleDist);
          createTransformRequest(call, aggregate, input, traits);
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

      HashAggPrel phase1Agg = HashAggPrel.create(
          aggregate.getCluster(),
          traits,
          newInput,
          aggregate.getGroupSet(),
          aggregate.getGroupSets(),
          getUpdatedPhase1AggregateCall(aggregate),
          OperatorPhase.PHASE_1of2);

      HashToRandomExchangePrel exch =
          new HashToRandomExchangePrel(phase1Agg.getCluster(), phase1Agg.getTraitSet().plus(Prel.PHYSICAL).plus(distOnAllKeys),
              phase1Agg, ImmutableList.copyOf(getDistributionField(aggregate, true)));

      final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();
      final List<AggregateCall> phase2Calls = new ArrayList<>();
      final List<RexNode> projectedExprs = new ArrayList<>(rexBuilder.identityProjects(exch.getRowType()));
      final List<String> fieldNames = new ArrayList<>(exch.getRowType().getFieldNames());
      int delimiterCount = 0;
      int startIndex = MoreRelOptUtil.getNextExprIndexFromFields(fieldNames);

      // handle delimiter constant for ListAgg
      for (Pair<AggregateCall, RexLiteral> aggPair : phase1Agg.getPhase2AggCalls()) {
        final AggregateCall call = aggPair.getKey();
        final RexLiteral literal = aggPair.getValue();
        if (call.getAggregation().getKind() == SqlKind.LISTAGG && literal != null) {
          projectedExprs.add(literal);
          fieldNames.add("EXPR$" + (startIndex + delimiterCount));
          delimiterCount++;
          phase2Calls.add(
            AggregateCall.create(
                call.getAggregation(),
                call.isDistinct(),
                call.isApproximate(),
                call.getArgList(),
                call.filterArg,
                call.collation,
                call.type,
                call.name));
        } else {
          phase2Calls.add(call);
        }
      }

      RelNode twoPhaseAggInput = exch;
      if (delimiterCount > 0) {
        RelDataType rowType = RexUtil.createStructType(exch.getCluster().getTypeFactory(), projectedExprs, fieldNames);
        twoPhaseAggInput = ProjectPrel.create(exch.getCluster(), exch.getTraitSet(), exch, projectedExprs, rowType);
      }

      HashAggPrel phase2Agg =  HashAggPrel.create(
          twoPhaseAggInput.getCluster(),
          twoPhaseAggInput.getTraitSet(),
          twoPhaseAggInput,
          phase1Agg.getPhase2GroupSet(),
          null,
          phase2Calls,
          OperatorPhase.PHASE_2of2);
      return phase2Agg;
    }
  }

  private void createTransformRequest(RelOptRuleCall call, AggregateRel aggregate,
                                      RelNode input, RelTraitSet traits) throws InvalidRelException {

    final RelNode convertedInput = convert(input, PrelUtil.fixTraits(call, traits));

    HashAggPrel newAgg = HashAggPrel.create(
        aggregate.getCluster(),
        traits,
        convertedInput,
        aggregate.getGroupSet(),
        aggregate.getGroupSets(),
        aggregate.getAggCallList(),
        OperatorPhase.PHASE_1of1);

    call.transformTo(newAgg);
  }

}
