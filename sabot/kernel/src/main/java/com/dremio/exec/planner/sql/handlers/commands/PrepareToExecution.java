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
package com.dremio.exec.planner.sql.handlers.commands;

import com.dremio.exec.physical.PhysicalPlan;
import com.dremio.exec.planner.observer.AttemptObserver;

/** Go from prepare to execution. */
public class PrepareToExecution extends AsyncCommand {

  private final PreparedPlan plan;
  private final AttemptObserver observer;

  public PrepareToExecution(PreparedPlan plan, AttemptObserver observer) {
    super();
    this.plan = plan;
    this.observer = observer;
  }

  @Override
  public PhysicalPlan getPhysicalPlan() {
    return plan.getPlan();
  }

  @Override
  public double plan() {
    plan.replay(observer);
    return plan.getPlan().getCost();
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
