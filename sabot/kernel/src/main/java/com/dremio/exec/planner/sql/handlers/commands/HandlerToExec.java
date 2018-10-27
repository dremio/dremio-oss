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
package com.dremio.exec.planner.sql.handlers.commands;

import org.apache.calcite.sql.SqlNode;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.fragment.PlanningSet;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;
import com.dremio.resource.ResourceAllocator;

/**
 * Take a sql node and run as async command.
 */
public class HandlerToExec extends AsyncCommand<Object> {

  private final CoordToExecTunnelCreator tunnelCreator;
  private final PhysicalPlanReader reader;
  private final AttemptObserver observer;
  private final SqlNode sqlNode;
  private final SqlToPlanHandler handler;
  private final String sql;
  private final SqlHandlerConfig config;

  private ExecutionPlan exec;

  public HandlerToExec(
    CoordToExecTunnelCreator tunnelCreator,
    QueryContext context,
    PhysicalPlanReader reader,
    AttemptObserver observer,
    String sql,
    SqlNode sqlNode,
    SqlToPlanHandler handler,
    SqlHandlerConfig config,
    ResourceAllocator queryResourceManager) {
    super(context, queryResourceManager, observer);
    this.tunnelCreator = tunnelCreator;
    this.reader = reader;
    this.observer = observer;
    this.sqlNode = sqlNode;
    this.sql = sql;
    this.handler = handler;
    this.config = config;
  }

  @Override
  public double plan() throws Exception {
    observer.planStart(sql);
    PhysicalPlan plan = handler.getPlan(config, sql, sqlNode);
    final PlanningSet planningSet = allocateResourcesBasedOnPlan(plan);
    exec = ExecutionPlanCreator.getExecutionPlan(context, reader, observer, plan,
      resourceSet, planningSet);
    observer.planCompleted(exec);
    return plan.getCost();
  }

  @Override
  public Object execute() throws Exception {
    FragmentStarter starter = new FragmentStarter(tunnelCreator, resourceSchedulingDecisionInfo);
    starter.start(exec, observer);
    return null;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ASYNC_QUERY;
  }

  @Override
  public String getDescription() {
    return "execute; query";
  }

}
