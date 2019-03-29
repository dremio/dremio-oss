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

import java.util.concurrent.TimeUnit;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexExecutor;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider.SubstitutionStream;
import com.dremio.exec.planner.logical.CancelFlag;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.service.Pointer;
import com.google.common.base.Throwables;

public class DremioVolcanoPlanner extends VolcanoPlanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioVolcanoPlanner.class);

  private final SubstitutionProvider substitutionProvider;

  private final CancelFlag cancelFlag;
  private PlannerPhase phase;
  private MaxNodesListener listener;

  private DremioVolcanoPlanner(RelOptCostFactory costFactory, Context context, SubstitutionProvider substitutionProvider) {
    super(costFactory, context);
    this.substitutionProvider = substitutionProvider;
    this.cancelFlag = new CancelFlag(context.unwrap(PlannerSettings.class).getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
    this.phase = null;
    this.listener = new MaxNodesListener(context.unwrap(PlannerSettings.class).getMaxNodesPerPlan());
    addListener(listener);
  }

  public static DremioVolcanoPlanner of(final SqlConverter converter) {
    final ConstExecutor executor = new ConstExecutor(converter.getFunctionImplementationRegistry(), converter.getFunctionContext(), converter.getSettings());

    return of(converter.getCostFactory(), converter.getSettings(), converter.getSubstitutionProvider(), executor);
  }

  public static DremioVolcanoPlanner of(RelOptCostFactory costFactory, Context context, SubstitutionProvider substitutionProvider, RexExecutor executor) {
    DremioVolcanoPlanner volcanoPlanner = new DremioVolcanoPlanner(costFactory, context, substitutionProvider);
    volcanoPlanner.setExecutor(executor);
    volcanoPlanner.clearRelTraitDefs();
    volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    volcanoPlanner.addRelTraitDef(DistributionTraitDef.INSTANCE);
    volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

    return volcanoPlanner;
  }

  @Override
  public RelNode findBestExp() {
    try {
      cancelFlag.reset();
      listener.reset();
      return super.findBestExp();
    } catch(RuntimeException ex) {
      // if the planner is hiding a UserException, bubble it's message to the top.
      Throwable t = Throwables.getRootCause(ex);
      if(t instanceof UserException) {
        throw UserException.parseError(ex).message(t.getMessage()).build(logger);
      } else {
        throw ex;
      }
    }
  }

  public void setPlannerPhase(PlannerPhase phase) {
    this.phase = phase;
  }

  @Override
  protected void registerMaterializations() {
    final SubstitutionStream result = substitutionProvider.findSubstitutions(getOriginalRoot());
    Pointer<Integer> count = new Pointer<>(0);
    try {
      result.stream().forEach(substitution -> {
        count.value++;
          if (!isRegistered(substitution.getReplacement())) {
            RelNode equiv = substitution.considerThisRootEquivalent() ? getRoot() : substitution.getEquivalent();
            register(substitution.getReplacement(), ensureRegistered(equiv, null));
          }
      });
    } catch (Exception | AssertionError e) {
      result.failure(e);
      logger.debug("found {} substitutions", count.value);
      return;
    }
    result.success();
    logger.debug("found {} substitutions", count.value);
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
