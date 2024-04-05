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

/** Base class for Asynchronous queries. */
public abstract class AsyncCommand implements CommandRunner<Void> {

  public AsyncCommand() {}

  @Override
  public CommandType getCommandType() {
    return CommandType.ASYNC_QUERY;
  }

  public abstract PhysicalPlan getPhysicalPlan();

  public void executionStarted() {}

  @Override
  public Void execute() {
    // TODO (DX-16022) refactor the code to no longer require this
    throw new IllegalStateException("Should never be called");
  }

  @Override
  public void close() throws Exception {}
}
