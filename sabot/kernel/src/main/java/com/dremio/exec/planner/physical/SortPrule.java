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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;

import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.logical.SortRel;
import com.dremio.exec.planner.physical.DistributionTrait.DistributionField;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

/**
 *
 * Rule that converts a logical {@link SortRel} to a physical sort.  Convert from Logical Sort into Physical Sort.
 * For Logical Sort, it requires one single data stream as the output.
 *
 */
public class SortPrule extends Prule{
  public static final RelOptRule INSTANCE = new SortPrule();

  private SortPrule() {
    super(RelOptHelper.any(SortRel.class, Rel.LOGICAL), "Prel.SortPrule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final SortRel sort = (SortRel) call.rel(0);
    final RelNode input = sort.getInput();

    final PlannerSettings plannerSettings = PrelUtil.getPlannerSettings(call.getPlanner());
    final RelTraitSet inputTraits;

    if (plannerSettings.getOptions().getOption(PlannerSettings.ENABLE_SORT_ROUND_ROBIN)) {
      // Keep the collation in logical sort just make its input round robin round robin.
      inputTraits = sort.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.ROUND_ROBIN);
    } else {
      // Keep the collation in logical sort. Convert input into a RelNode with 1) this collation, 2) Physical, 3) hash distributed on
      DistributionTrait hashDistribution =
        new DistributionTrait(DistributionTrait.DistributionType.HASH_DISTRIBUTED, ImmutableList.copyOf(getDistributionField(sort)));
      inputTraits = sort.getTraitSet().plus(Prel.PHYSICAL).plus(hashDistribution);
    }

    final RelNode convertedInput = convert(input, inputTraits);

    if(isSingleMode(call)){
      call.transformTo(convertedInput);
    }else{
      RelNode exch = new SingleMergeExchangePrel(sort.getCluster(), sort.getTraitSet().plus(Prel.PHYSICAL).plus(DistributionTrait.SINGLETON), convertedInput, sort.getCollation());
      call.transformTo(exch);  // transform logical "sort" into "SingleMergeExchange".

    }
  }

  private List<DistributionField> getDistributionField(SortRel rel) {
    List<DistributionField> distFields = Lists.newArrayList();

    for (RelFieldCollation relField : rel.getCollation().getFieldCollations()) {
      DistributionField field = new DistributionField(relField.getFieldIndex());
      distFields.add(field);
    }

    return distFields;
  }
}
