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
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.GroupManager;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.Collection;
import org.apache.arrow.memory.BufferAllocator;

/** Storage and access of {@link WorkloadTicket}s -- one per workload */
public class WorkloadTicketDepot implements AutoCloseable {
  private static final String INSTANT_MAX_ALLOCATION_CONFIG = "allocators.instant.max";
  private static final String BACKGROUND_MAX_ALLOCATION_CONFIG = "allocators.background.max";
  private static final String GENERAL_MAX_ALLOCATION_CONFIG = "allocators.general.max";

  private static final long NRT_WEIGHT = 1000;
  private static final long GENERAL_WEIGHT = 100;
  private static final long BACKGROUND_WEIGHT = 1;

  protected final GroupManager<AsyncTaskWrapper> manager;

  private final WorkloadTicket nrtWorkloadTicket;
  private final WorkloadTicket generalWorkloadTicket;
  private final WorkloadTicket backgroundWorkloadTicket;

  public WorkloadTicketDepot(
      BufferAllocator parentAllocator, SabotConfig config, GroupManager<AsyncTaskWrapper> manager) {
    this.manager = Preconditions.checkNotNull(manager, "Task manager required");

    nrtWorkloadTicket =
        new WorkloadTicket(
            parentAllocator.newChildAllocator(
                "nrt-workload-allocator",
                0,
                getLongConfig(config, INSTANT_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE)),
            manager.newGroup(NRT_WEIGHT));
    nrtWorkloadTicket.reserve();

    generalWorkloadTicket =
        new WorkloadTicket(
            parentAllocator.newChildAllocator(
                "general-workload-allocator",
                0,
                getLongConfig(config, GENERAL_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE)),
            manager.newGroup(GENERAL_WEIGHT));
    generalWorkloadTicket.reserve();

    backgroundWorkloadTicket =
        new WorkloadTicket(
            parentAllocator.newChildAllocator(
                "background-workload-allocator",
                0,
                getLongConfig(config, BACKGROUND_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE)),
            manager.newGroup(BACKGROUND_WEIGHT));
    backgroundWorkloadTicket.reserve();
  }

  private static long getLongConfig(SabotConfig config, String path, long defaultValue) {
    if (config.hasPath(path)) {
      return config.getLong(path);
    }
    return defaultValue;
  }

  /**
   * Get the ticket that corresponds to the appropriate workload The ticket is always 'reserve()'d,
   * which means that callers should call release() on it once they're done using it
   */
  public WorkloadTicket getWorkloadTicket(final SchedulingInfo schedulingInfo) {
    WorkloadTicket result;
    switch (schedulingInfo.getWorkloadClass()) {
      case BACKGROUND:
        result = backgroundWorkloadTicket;
        break;
      case GENERAL:
        result = generalWorkloadTicket;
        break;
      case NRT:
        result = nrtWorkloadTicket;
        break;
      default: // REALTIME priority is not handled for now
        throw new IllegalStateException("Unknown work class: " + schedulingInfo.getWorkloadClass());
    }
    result.reserve();
    return result;
  }

  /**
   * @return all the active query tickets
   */
  public Collection<WorkloadTicket> getWorkloadTickets() {
    return ImmutableList.of(nrtWorkloadTicket, generalWorkloadTicket, backgroundWorkloadTicket);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(
        (nrtWorkloadTicket.release() ? nrtWorkloadTicket : null),
        (backgroundWorkloadTicket.release() ? backgroundWorkloadTicket : null),
        (generalWorkloadTicket.release() ? generalWorkloadTicket : null));
  }
}
