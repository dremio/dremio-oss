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

import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.sql.SqlOperatorTable;

import com.dremio.exec.expr.fn.FunctionImplementationRegistry;
import com.dremio.exec.ops.UserDefinedFunctionExpander;
import com.dremio.exec.planner.HepPlannerRunner;
import com.dremio.exec.planner.logical.ConstExecutor;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.physical.PlannerSettings;
import com.dremio.exec.planner.sql.SqlConverter;
import com.dremio.sabot.exec.context.FunctionContext;

public class PlannerBaseComponentImpl implements PlannerBaseComponent {

  private final PlannerSettings plannerSettings;
  private final FunctionImplementationRegistry functionImplementationRegistry;
  private final FunctionContext functionContext;
  private final SqlOperatorTable sqlOperatorTable;
  private final SqlConverter sqlConverter;
  private final HepPlannerRunner hepPlannerRunner;
  private final UserDefinedFunctionExpander userDefinedFunctionExpander;

  public PlannerBaseComponentImpl(
      PlannerSettings plannerSettings,
      FunctionImplementationRegistry functionImplementationRegistry,
      FunctionContext functionContext,
      SqlOperatorTable sqlOperatorTable,
      SqlConverter sqlConverter,
      HepPlannerRunner hepPlannerRunner,
      UserDefinedFunctionExpander userDefinedFunctionExpander) {
    this.plannerSettings = plannerSettings;
    this.functionImplementationRegistry = functionImplementationRegistry;
    this.functionContext = functionContext;
    this.sqlOperatorTable = sqlOperatorTable;
    this.sqlConverter = sqlConverter;
    this.hepPlannerRunner = hepPlannerRunner;
    this.userDefinedFunctionExpander = userDefinedFunctionExpander;
  }

  @Override
  public PlannerSettings getPlannerSettings() {
    return plannerSettings;
  }

  @Override
  public FunctionImplementationRegistry getFunctionImplementationRegistry() {
    return functionImplementationRegistry;
  }

  @Override
  public FunctionContext getFunctionContext() {
    return functionContext;
  }

  @Override
  public SqlConverter getSqlConverter() {
    return sqlConverter;
  }

  @Override
  public SqlOperatorTable getSqlOperatorTable() {
    return sqlOperatorTable;
  }


  @Override
  public HepPlannerRunner getHepPlannerRunner() {
    return hepPlannerRunner;
  }

  @Override
  public UserDefinedFunctionExpander getUserDefinedFunctionExpander() {
    return userDefinedFunctionExpander;
  }

  public static PlannerBaseComponent build(
      PlannerBaseModule plannerBaseModule,
      PlannerSettings plannerSettings,
      FunctionImplementationRegistry functionImplementationRegistry,
      FunctionContext functionContext,
      SqlOperatorTable sqlOperatorTable,
      SqlConverter sqlConverter,
      AttemptObserver attemptObserver) {
    ConstExecutor constExecutor =
      plannerBaseModule.buildConstExecutor(
        plannerSettings,
        functionImplementationRegistry,
        functionContext);
    RelOptCostFactory relOptCostFactory = plannerBaseModule.buildRelOptCostFactory(plannerSettings);
    HepPlannerRunner.PlannerStatsReporter plannerStatsReporter =
      plannerBaseModule.buildPlannerStatsReporter(attemptObserver);

    HepPlannerRunner hepPlannerRunner = plannerBaseModule.buildHepPlannerRunner(
      plannerSettings,
      constExecutor,
      relOptCostFactory,
      plannerStatsReporter);
    UserDefinedFunctionExpander userDefinedFunctionExpander =
      plannerBaseModule.buildUserDefinedFunctionExpander(sqlConverter);
    return new PlannerBaseComponentImpl(
      plannerSettings,
      functionImplementationRegistry,
      functionContext,
      sqlOperatorTable,
      sqlConverter,
      hepPlannerRunner,
      userDefinedFunctionExpander);
  }

}
