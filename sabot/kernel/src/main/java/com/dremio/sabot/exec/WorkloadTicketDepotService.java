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

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.config.DremioConfig;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.GroupManager;
import com.dremio.sabot.task.TaskPool;
import com.dremio.service.Service;
import com.google.common.annotations.VisibleForTesting;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Service that creates the {@link WorkloadTicketDepot} singleton when started, and provides it
 * through the registry
 */
@Singleton
public class WorkloadTicketDepotService implements Service {

  private final Provider<BufferAllocator> allocator;
  private final Provider<TaskPool> taskPool;
  private final Provider<DremioConfig> dremioConfig;

  private WorkloadTicketDepot ticketDepot;

  @Inject
  public WorkloadTicketDepotService(
      final Provider<BufferAllocator> allocator,
      final Provider<TaskPool> taskPool,
      final Provider<DremioConfig> dremioConfig) {
    this.allocator = allocator;
    this.taskPool = taskPool;
    this.dremioConfig = dremioConfig;
    ticketDepot = null;
  }

  @Override
  public void start() throws Exception {
    ticketDepot = newTicketDepot(allocator.get(), dremioConfig.get().getSabotConfig());
  }

  @Override
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

  protected WorkloadTicketDepot newTicketDepot(
      BufferAllocator parentAllocator, SabotConfig config) {
    return new WorkloadTicketDepot(parentAllocator, config, getGroupManager());
  }
}
