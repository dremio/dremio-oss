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

import static com.dremio.exec.work.foreman.AttemptManager.INJECTOR_DURING_PLANNING_PAUSE;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.MulticastRelOptListener;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.runtime.CalciteException;

import com.dremio.common.exceptions.UserException;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider;
import com.dremio.exec.planner.acceleration.substitution.SubstitutionProvider.SubstitutionStream;
import com.dremio.exec.planner.logical.CancelFlag;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.physical.DistributionTraitDef;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.exec.testing.ControlsInjector;
import com.dremio.exec.testing.ControlsInjectorFactory;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.service.Pointer;
import com.google.common.base.Throwables;

public class DremioVolcanoPlanner extends VolcanoPlanner {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DremioVolcanoPlanner.class);
  private static final ControlsInjector INJECTOR = ControlsInjectorFactory.getInjector(DremioVolcanoPlanner.class);

  private final SubstitutionProvider substitutionProvider;

  private final CancelFlag cancelFlag;

  private RelNode originalRoot;
  private PlannerPhase phase;
  private final MaxNodesListener maxNodesListener;
  private final MatchCountListener matchCountListener;
  private final ExecutionControls executionControls;
  private final PlannerSettings plannerSettings;

  private DremioVolcanoPlanner(RelOptCostFactory costFactory, Context context, SubstitutionProvider substitutionProvider) {
    super(costFactory, context);
    this.substitutionProvider = substitutionProvider;
    plannerSettings = context.unwrap(PlannerSettings.class);
    this.cancelFlag = new CancelFlag(plannerSettings.getMaxPlanningPerPhaseMS());
    this.executionControls = plannerSettings.unwrap(ExecutionControls.class);
    this.phase = null;
    this.maxNodesListener = new MaxNodesListener(plannerSettings.getMaxNodesPerPlan());
    this.matchCountListener = new MatchCountListener((int) plannerSettings.getOptions().getOption(PlannerSettings.HEP_PLANNER_MATCH_LIMIT),
      plannerSettings.getOptions().getOption(PlannerSettings.VERBOSE_RULE_MATCH_LISTENER));
    // A hacky way to add listeners to first multicast listener and register that listener to the Volcano planner.
    // The Volcano planner currently only supports a single listener. Need to update that to use the multi class
    // listener from its super class AbstractRelOptPlanner.
    MulticastRelOptListener listener = new MulticastRelOptListener();
    listener.addListener(maxNodesListener);
    listener.addListener(matchCountListener);
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
      maxNodesListener.reset();
      matchCountListener.reset();
      return super.findBestExp();
    } catch(RuntimeException ex) {
      // if the planner is hiding a UserException, bubble it's message to the top.
      Throwable t = Throwables.getRootCause(ex);
      if(t instanceof UserException) {
        throw UserException.parseError(ex).message(t.getMessage()).build(logger);
      } else {
        throw ex;
      }
    } finally {
      cancelFlag.stop();
    }
  }

  public void setPlannerPhase(PlannerPhase phase) {
    this.phase = phase;
  }

  @Override
  protected void registerMaterializations() {
    SubstitutionStream result;
    try {
      result = substitutionProvider.findSubstitutions(getOriginalRoot());
    } catch (RuntimeException ex) {
      logger.debug(ex.getMessage());
      throw ex;
    }
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
      ExceptionUtils.throwUserException(String.format("Query was cancelled because planning time exceeded %d seconds",
                                                      cancelFlag.getTimeoutInSecs()),
                                        null, plannerSettings, phase, logger);
    }

    if (executionControls != null) {
      INJECTOR.injectPause(executionControls, INJECTOR_DURING_PLANNING_PAUSE, logger);
    }

    try {
      super.checkCancel();
    } catch (CalciteException e) {
      if (plannerSettings.isCancelledByHeapMonitor()) {
        ExceptionUtils.throwUserException(plannerSettings.getCancelReason(), e, plannerSettings, phase, logger);
      } else {
        ExceptionUtils.throwUserCancellationException(plannerSettings);
      }
    }
  }

  @Override
  public RelNode getOriginalRoot() {
    return originalRoot;
  }

  @Override
  public void setOriginalRoot(RelNode originalRoot) {
    this.originalRoot = originalRoot;
  }

  @Override
  public void setRoot(RelNode rel) {
    if (originalRoot == null) {
      originalRoot = rel;
    }
    super.setRoot(rel);
  }

  public MatchCountListener getMatchCountListener() {
    return matchCountListener;
  }
}
