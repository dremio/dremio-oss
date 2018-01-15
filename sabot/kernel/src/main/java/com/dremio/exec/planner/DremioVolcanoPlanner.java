/*
 * Copyright (C) 2017 Dremio Corporation
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

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Pair;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider.Substitution;
import com.dremio.exec.planner.logical.CancelFlag;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.sql.SqlConverter;

public class DremioVolcanoPlanner extends VolcanoPlanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioVolcanoPlanner.class);

  private final SubstitutionProvider substitutionProvider;

  private static final boolean IS_DEBUG = java.lang.management.ManagementFactory.getRuntimeMXBean().getInputArguments()
      .toString().indexOf("-agentlib:jdwp") > 0;


  private CancelFlag cancelFlag = null;

  public DremioVolcanoPlanner(final SqlConverter converter) {
    super(converter.getCostFactory(), converter.getSettings());
    this.substitutionProvider = converter.getSubstitutionProvider();

    setExecutor(new ConstExecutor(converter.getFunctionImplementationRegistry(), converter.getFunctionContext(), converter.getSettings()));
    clearRelTraitDefs();
    addRelTraitDef(ConventionTraitDef.INSTANCE);
    addRelTraitDef(DistributionTraitDef.INSTANCE);
    addRelTraitDef(RelCollationTraitDef.INSTANCE);
  }

  @Override
  protected void useApplicableMaterializations() {
    final List<Substitution> substitutions = substitutionProvider.findSubstitutions(originalRoot);
    LOGGER.debug("found {} substitutions", substitutions.size());
    for (final Substitution substitution : substitutions) {
      if (!isRegistered(substitution.getReplacement())) {
        RelNode equiv = substitution.getEquivalent();
        if (equiv == null) {
          equiv = getRoot();
        }
        Hook.SUB.run(substitution);
        register(substitution.getReplacement(), ensureRegistered(equiv, null));
      }
    }
  }

  @Override
  public RelNode findBestExp() {
    final RelNode bestExp = super.findBestExp();
    return substitutionProvider.processPostPlanning(bestExp);
  }

  public void setCancelFlag(CancelFlag cancelFlag) {
    this.cancelFlag = cancelFlag;
  }

  @Override
  public void checkCancel() {
    if(!IS_DEBUG){
      if (cancelFlag != null && cancelFlag.isCancelRequested()) {
        throw UserException.planError()
            .message("Query was cancelled because planning time exceeded %d seconds", cancelFlag.getTimeoutInSecs())
            .addContext("Planner Phase", cancelFlag.getPlannerPhase().description)
            .build(logger);
      }
      super.checkCancel();
    }
  }
}
