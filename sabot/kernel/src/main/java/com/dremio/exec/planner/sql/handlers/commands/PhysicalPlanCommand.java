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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.PhysicalPlanReader;
import com.dremio.exec.planner.observer.AttemptObserver;
import com.dremio.exec.work.foreman.ExecutionPlan;
import com.dremio.exec.work.rpc.CoordToExecTunnelCreator;

// should be deprecated once tests are removed.
public class PhysicalPlanCommand extends AsyncCommand<Object> {

  private final CoordToExecTunnelCreator tunnelCreator;
  private final QueryContext context;
  private final PhysicalPlanReader reader;
  private final AttemptObserver observer;
  private final String plan;

  private ExecutionPlan exec;

  public PhysicalPlanCommand(
      CoordToExecTunnelCreator tunnelCreator,
      QueryContext context,
      PhysicalPlanReader reader,
      AttemptObserver observer,
      String plan) {
    super(context);
    this.tunnelCreator = tunnelCreator;
    this.context = context;
    this.reader = reader;
    this.observer = observer;
    this.plan = plan;
  }

  @Override
  public double plan() throws Exception {
    PhysicalPlan plan = reader.readPhysicalPlan(this.plan);
    setQueueTypeFromPlan(plan);
    exec = ExecutionPlanCreator.getExecutionPlan(context, reader, observer, plan, getQueueType());
    observer.planCompleted(exec);
    return plan.getCost();
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
