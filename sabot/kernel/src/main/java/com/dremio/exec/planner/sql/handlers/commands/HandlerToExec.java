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
import com.dremio.exec.planner.sql.handlers.SqlHandlerConfig;
import com.dremio.exec.planner.sql.handlers.query.SqlToPlanHandler;
import org.apache.calcite.sql.SqlNode;

/** Take a sql node and run as async command. */
public class HandlerToExec extends AsyncCommand {

  private final AttemptObserver observer;
  private final SqlNode sqlNode;
  private SqlToPlanHandler handler;
  private final String sql;
  private SqlHandlerConfig config;
  private PhysicalPlan physicalPlan;

  public HandlerToExec(
      AttemptObserver observer,
      String sql,
      SqlNode sqlNode,
      SqlToPlanHandler handler,
      SqlHandlerConfig config) {
    super();
    this.observer = observer;
    this.sqlNode = sqlNode;
    this.sql = sql;
    this.handler = handler;
    this.config = config;
  }

  @Override
  public PhysicalPlan getPhysicalPlan() {
    return physicalPlan;
  }

  @Override
  public double plan() throws Exception {
    observer.planStart(sql);
    physicalPlan = handler.getPlan(config, sql, sqlNode);
    return physicalPlan.getCost();
  }

  @Override
  public void executionStarted() {
    physicalPlan = null; // no longer needed.
    config = null; // no longer needed.
    handler = null; // no longer needed.
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
