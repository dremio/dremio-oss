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
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider.Substitution;
import com.dremio.exec.planner.logical.CancelFlag;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;

public class DremioVolcanoPlanner extends VolcanoPlanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioVolcanoPlanner.class);

  private final SubstitutionProvider substitutionProvider;

  private final CancelFlag cancelFlag;
  private PlannerPhase phase;

  private DremioVolcanoPlanner(RelOptCostFactory costFactory, Context context, SubstitutionProvider substitutionProvider) {
    super(costFactory, context);
    this.substitutionProvider = substitutionProvider;
    this.cancelFlag = new CancelFlag(context.unwrap(PlannerSettings.class).getMaxPlanningPerPhaseMS(), TimeUnit.MILLISECONDS);
    this.phase = null;
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
    cancelFlag.reset();
    return super.findBestExp();
  }

  public void setPlannerPhase(PlannerPhase phase) {
    this.phase = phase;
  }

  @Override
  protected void registerMaterializations() {
    final List<Substitution> substitutions = substitutionProvider.findSubstitutions(getOriginalRoot());
    LOGGER.debug("found {} substitutions", substitutions.size());
    for (final Substitution substitution : substitutions) {
      if (!isRegistered(substitution.getReplacement())) {
        RelNode equiv = substitution.considerThisRootEquivalent() ? getRoot() : substitution.getEquivalent();
        register(substitution.getReplacement(), ensureRegistered(equiv, null));
      }
    }
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
