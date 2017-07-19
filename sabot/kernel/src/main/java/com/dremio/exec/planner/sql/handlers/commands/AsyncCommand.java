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

import com.dremio.exec.ExecConstants;
import com.dremio.exec.ops.QueryContext;
import com.dremio.exec.physical.PhysicalPlan;

/**
 * Base class for Asynchronous queries.
 */
public abstract class AsyncCommand<T> implements CommandRunner<T> {

  public enum QueueType {
    SMALL,
    LARGE
  }

  protected final QueryContext context;

  private QueueType queueType;

  public AsyncCommand(QueryContext context) {
    this.context = context;
  }

  @Override
  public CommandType getCommandType() {
    return CommandType.ASYNC_QUERY;
  }

  protected void setQueueTypeFromPlan(PhysicalPlan plan) {
    final long queueThreshold = context.getOptions().getOption(ExecConstants.QUEUE_THRESHOLD_SIZE);
    queueType = (plan.getCost() > queueThreshold) ? QueueType.LARGE : QueueType.SMALL;
  }

  public QueueType getQueueType() {
    return queueType;
  }
}
