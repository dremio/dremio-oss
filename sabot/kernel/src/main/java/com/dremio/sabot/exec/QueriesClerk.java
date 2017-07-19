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
package com.dremio.sabot.exec;

import java.util.Map;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.exec.proto.helper.QueryIdHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

/**
 * Manages workload and query level allocators
 */
public class QueriesClerk implements AutoCloseable {

  private static final String INSTANT_MAX_ALLOCATION_CONFIG = "allocators.instant.max";
  private static final String BACKGROUND_MAX_ALLOCATION_CONFIG = "allocators.background.max";
  private static final String GENERAL_MAX_ALLOCATION_CONFIG = "allocators.general.max";

  private final BufferAllocator nrtAllocator;
  private final BufferAllocator generalAllocator;
  private final BufferAllocator backgroundAllocator;

  private final Map<QueryId, QueryClerk> clerks = Maps.newHashMap();

  QueriesClerk(BufferAllocator parentAllocator, SabotConfig config) {
    nrtAllocator = parentAllocator.newChildAllocator("nrt-workload-allocator", 0,
      getLongConfig(config, INSTANT_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE));
    generalAllocator = parentAllocator.newChildAllocator("general-workload-allocator", 0,
      getLongConfig(config, GENERAL_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE));
    backgroundAllocator = parentAllocator.newChildAllocator("background-workload-allocator", 0,
      getLongConfig(config, BACKGROUND_MAX_ALLOCATION_CONFIG, Long.MAX_VALUE));
  }

  private static long getLongConfig(SabotConfig config, String path, long defaultValue) {
    if (config.hasPath(path)) {
      return config.getLong(path);
    }
    return defaultValue;
  }

  /**
   * creates a fragment ticket for the passed fragment, may create a new query clerk if no clerk is already
   * cached. Closing the ticket will return the reservation and eventually close the corresponding clerk along with
   * its allocator
   *
   * @param fragment fragment plan
   * @return reserved query allocator
   */
  public FragmentTicket newFragmentTicket(PlanFragment fragment) {
    final QueryId queryId = fragment.getHandle().getQueryId();
    final WorkloadClass workloadClass = fragment.getPriority().getWorkloadClass();
    final long maxAllocation = fragment.getContext().getQueryMaxAllocation();

    synchronized (clerks) {

      QueryClerk queryClerk = clerks.get(queryId);
      if (queryClerk == null) {
        queryClerk = newQueryAllocator(queryId, workloadClass, maxAllocation);
        clerks.put(queryId, queryClerk);
      }

      return new FragmentTicket(queryClerk);
    }
  }

  private QueryClerk newQueryAllocator(QueryId queryId, WorkloadClass workloadClass, long maxAllocation) {
    final BufferAllocator workloadAllocator;
    switch (workloadClass) {
      case BACKGROUND:
        workloadAllocator = backgroundAllocator;
        break;
      case GENERAL:
        workloadAllocator = generalAllocator;
        break;
      case NRT:
        workloadAllocator = nrtAllocator;
        break;
      default: // REALTIME priority is not handled for now
        throw new IllegalStateException("Unknown work class: " + workloadClass);
    }

    final BufferAllocator childAllocator = workloadAllocator.newChildAllocator("query-" + QueryIdHelper.getQueryId(queryId),
      0, maxAllocation);
    return new QueryClerk(queryId, childAllocator);
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(nrtAllocator, backgroundAllocator, generalAllocator);
  }

  public class FragmentTicket implements AutoCloseable {
    private QueryClerk clerk;
    private boolean closed;

    private FragmentTicket(QueryClerk clerk) {
      this.clerk = Preconditions.checkNotNull(clerk, "QueryClerk should not be null");
      clerk.reserve();
    }

    public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
      return clerk.getAllocator().newChildAllocator(name, initReservation, maxAllocation);
    }

    @Override
    public void close() throws Exception {
      Preconditions.checkState(!closed, "Trying to close FragmentTicket more than once");
      closed = true;

      synchronized (clerks) {
        if (clerk.release()) {
          final QueryClerk queryClerk = clerks.remove(clerk.getQueryId());
          Preconditions.checkState(queryClerk == clerk,
            "closed query allocator was not found in the query allocators' map");

          AutoCloseables.close(clerk);
        }
      }
    }
  }
}
