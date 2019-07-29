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
package com.dremio.exec.planner;

import java.util.List;
import java.util.concurrent.TimeUnit;

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
import org.apache.calcite.rel.RelNode;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.logical.CancelFlag;
import com.dremio.exec.planner.physical.DistributionTrait;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

public class DremioHepPlanner extends HepPlanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioHepPlanner.class);

  private final CancelFlag cancelFlag;
  private final PlannerPhase phase;
  private final MaxNodesListener listener;

  public DremioHepPlanner(final HepProgram program, final Context context, final RelOptCostFactory costFactory, PlannerPhase phase) {
    super(program, context, false, null, costFactory);
    this.cancelFlag = new CancelFlag(context.unwrap(PlannerSettings.class).getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
    this.phase = phase;
    this.listener = new MaxNodesListener(context.unwrap(PlannerSettings.class).getMaxNodesPerPlan());
    addListener(listener);
  }

  @Override
  public RelNode findBestExp() {
    try {
      cancelFlag.reset();
      listener.reset();
      return super.findBestExp();
    } catch(RuntimeException ex) {
      // if the planner is hiding a UserException, bubble its message to the top.
      Throwable t = Throwables.getRootCause(ex);
      if(t instanceof UserException) {
        throw UserException.parseError(ex).message(t.getMessage()).build(logger);
      } else {
        throw ex;
      }
    }
  }

  @Override
  public RelTraitSet emptyTraitSet() {
    return RelTraitSet.createEmpty().plus(Convention.NONE).plus(DistributionTrait.DEFAULT).plus(RelCollations.EMPTY);
  }

  @Override
  public List<RelTraitDef> getRelTraitDefs() {
    return ImmutableList.<RelTraitDef>of(ConventionTraitDef.INSTANCE, DistributionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
  }

  @Override
  public void checkCancel() {
    if (cancelFlag.isCancelRequested()) {
      UserException.Builder builder = UserException.planError()
          .message("Query was cancelled because planning time exceeded %d seconds", cancelFlag.getTimeoutInSecs());
      if (phase != null) {
        builder = builder.addContext("Planner Phase", phase.description);
      }
      throw builder.build(logger);
    }
    super.checkCancel();
  }

}
