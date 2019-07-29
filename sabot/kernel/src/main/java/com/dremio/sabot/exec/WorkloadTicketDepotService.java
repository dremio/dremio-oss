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
package com.dremio.sabot.exec;

import javax.inject.Provider;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.server.BootStrapContext;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.GroupManager;
import com.dremio.sabot.task.TaskPool;
import com.dremio.service.BindingCreator;
import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;

/**
 * Service that creates the {@link WorkloadTicketDepot} singleton when started, and provides it through the registry
 */
public class WorkloadTicketDepotService implements Service {
  private final BootStrapContext context;
  private final BindingCreator bindingCreator;

  private final Provider<TaskPool> taskPool;

  private WorkloadTicketDepot ticketDepot;

  public WorkloadTicketDepotService(final BootStrapContext context,
                                    final BindingCreator bindingCreator,
                                    final Provider<TaskPool> taskPool
                                    ) {
    this.context = context;
    this.bindingCreator = bindingCreator;
    this.taskPool = taskPool;
    ticketDepot = null;
  }

  public void start() throws Exception {
    ticketDepot = newTicketDepot(context.getAllocator(), context.getConfig());
    bindingCreator.bind(WorkloadTicketDepot.class, ticketDepot);
  }

  public void close() throws Exception {
    AutoCloseables.close(ticketDepot);
  }

  @VisibleForTesting
  public WorkloadTicketDepot getTicketDepot() {
    return ticketDepot;
  }

  protected GroupManager<AsyncTaskWrapper> getGroupManager() {
    return taskPool.get().getGroupManager();
  }

  protected WorkloadTicketDepot newTicketDepot(BufferAllocator parentAllocator, SabotConfig config) {
    return new WorkloadTicketDepot(parentAllocator, config, getGroupManager());
  }

  protected BindingCreator getBindingCreator() {
    return bindingCreator;
  }
}
