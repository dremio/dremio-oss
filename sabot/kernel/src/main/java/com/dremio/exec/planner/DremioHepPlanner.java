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
package com.dremio.exec.planner;

import java.util.List;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;

import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.google.common.collect.ImmutableList;

public class DremioHepPlanner extends HepPlanner {
  public DremioHepPlanner(final HepProgram program, final Context context, final RelOptCostFactory costFactory) {
    super(program, context, false, null, costFactory);
  }

  // HepPlanner doesn't check cancelFlag, so no need to override setCancelFlag/checkCancel.

  @Override
  public RelTraitSet emptyTraitSet() {
    return RelTraitSet.createEmpty().plus(Convention.NONE).plus(DistributionTrait.DEFAULT).plus(RelCollations.EMPTY);
  }

  @Override
  public List<RelTraitDef> getRelTraitDefs() {
    return ImmutableList.<RelTraitDef>of(ConventionTraitDef.INSTANCE, DistributionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
  }
}
