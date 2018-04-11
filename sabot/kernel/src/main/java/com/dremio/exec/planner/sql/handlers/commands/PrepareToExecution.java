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

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;

/**
 * Go from prepare to execution.
 */
public class PrepareToExecution extends AsyncCommand<Object> {

  private final PreparedPlan plan;
  private final QueryContext context;
  private final AttemptObserver observer;
  private final PhysicalPlanReader reader;
  private final CoordToExecTunnelCreator tunnelCreator;

  private ExecutionPlan exec;

  public PrepareToExecution(PreparedPlan plan, QueryContext context, AttemptObserver observer,
      PhysicalPlanReader reader,
      CoordToExecTunnelCreator tunnelCreator) {
    super(context);
    this.plan = plan;
    this.context = context;
    this.observer = observer;
    this.reader = reader;
    this.tunnelCreator = tunnelCreator;
  }

  @Override
  public double plan() throws Exception {
    plan.replay(observer);
    setQueueTypeFromPlan(plan.getPlan());
    exec = ExecutionPlanCreator.getExecutionPlan(context, reader, observer, plan.getPlan(), getQueueType());
    observer.planCompleted(exec);
    return plan.getPlan().getCost();
  }

  @Override
  public Object execute() throws Exception {
    FragmentStarter starter = new FragmentStarter(tunnelCreator);
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
