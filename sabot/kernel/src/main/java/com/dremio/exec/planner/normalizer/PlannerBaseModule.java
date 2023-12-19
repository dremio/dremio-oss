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
package com.dremio.exec.planner.normalizer;

import javax.annotation.Nullable;

import org.apache.calcite.plan.RelOptCostFactory;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.ops.UserDefinedFunctionExpanderImpl;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.cost.DremioCost;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.sabot.exec.context.FunctionContext;
import com.google.common.base.Preconditions;

public class PlannerBaseModule {

  @Nullable
  public RelOptCostFactory buildRelOptCostFactory(PlannerSettings plannerSettings) {
    return (plannerSettings.useDefaultCosting()) ? null : new DremioCost.Factory();
  }

  public ConstExecutor buildConstExecutor(
    PlannerSettings plannerSettings,
    FunctionImplementationRegistry funcImplReg,
    FunctionContext udfUtilities) {
    return new ConstExecutor(funcImplReg, udfUtilities, plannerSettings);
  }

  public HepPlannerRunner buildHepPlannerRunner(
    PlannerSettings plannerSettings,
    ConstExecutor constExecutor,
    @Nullable RelOptCostFactory relOptCostFactory,
    HepPlannerRunner.PlannerStatsReporter plannerStatsReporter) {
    return new HepPlannerRunner(
      plannerSettings,
      constExecutor,
      relOptCostFactory,
      plannerStatsReporter);
  }


  public HepPlannerRunner.PlannerStatsReporter buildPlannerStatsReporter(
    AttemptObserver attemptObserver
  ) {
    Preconditions.checkNotNull(attemptObserver);
    return (config, millisTaken, input, output, ruleToCount) -> attemptObserver.planRelTransform(
      config.getPlannerPhase(),
      null,
      input,
      output,
      millisTaken,
      ruleToCount);
  }

  public UserDefinedFunctionExpander buildUserDefinedFunctionExpander(SqlConverter sqlConverter) {
    return new UserDefinedFunctionExpanderImpl(sqlConverter);
  }
}
