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
package com.dremio.sabot.exec;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordExecRPC.PlanFragment;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.WorkloadClass;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Manages workload and query level allocators
 */
@ThreadSafe
public class QueriesClerk implements AutoCloseable {

  private static final String INSTANT_MAX_ALLOCATION_CONFIG = "allocators.instant.max";
  private static final String BACKGROUND_MAX_ALLOCATION_CONFIG = "allocators.background.max";
  private static final String GENERAL_MAX_ALLOCATION_CONFIG = "allocators.general.max";

  private final ExecToCoordTunnelCreator tunnelCreator;
  private final BufferAllocator nrtAllocator;
  private final BufferAllocator generalAllocator;
  private final BufferAllocator backgroundAllocator;

  private final ConcurrentMap<QueryId, QueryTicket> queryTickets = Maps.newConcurrentMap();

  QueriesClerk(BufferAllocator parentAllocator, SabotConfig config, ExecToCoordTunnelCreator tunnelCreator) {
    this.tunnelCreator = tunnelCreator;
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
   * Creates a query ticket (along with a query-level allocator) for a given query, if such a ticket has not already
   * been created.
   * Multi-thread safe
   */
  public QueryTicket getOrCreateQueryTicket(QueryId queryId, WorkloadClass workloadClass, long maxAllocation,
                                            NodeEndpoint foreman, NodeEndpoint assignment) {
    QueryTicket queryTicket = queryTickets.get(queryId);
    if (queryTicket == null) {
      final BufferAllocator queryAllocator =
        getWorkloadAllocator(workloadClass)
          .newChildAllocator("query-" + QueryIdHelper.getQueryId(queryId), 0, maxAllocation);
      queryTicket = new QueryTicket(this, queryId, queryAllocator, foreman, assignment, tunnelCreator);
      QueryTicket insertedTicket = queryTickets.putIfAbsent(queryId, queryTicket);
      if (insertedTicket != null) {
        // Race condition: another user managed to insert a query ticket. Let's close ours and use theirs
        Preconditions.checkState(insertedTicket != queryTicket);
        try {
          AutoCloseables.close(queryTicket);  // NB: closing the ticket will close the query allocator
        } catch (Exception e) {
          // Ignored
        }
        queryTicket = insertedTicket;
      }
    }
    return queryTicket;
  }

  /**
   * Remove a query ticket from this queries clerk
   * <p>
   * Multi-thread safe
   */
  public void removeQueryTicket(QueryTicket queryTicket) throws Exception {
    final QueryTicket removedQueryTicket = queryTickets.remove(queryTicket.getQueryId());
    Preconditions.checkState(removedQueryTicket == queryTicket,
      "closed query ticket was not found in the query tickets' map");
    AutoCloseables.close(queryTicket);
  }

  /**
   * creates a fragment ticket for the passed fragment, may create a new query ticket if no query ticket is already
   * cached. Closing the ticket will return the reservation and eventually close the corresponding query ticket along with
   * its allocator
   *
   * @param fragment fragment plan
   * @return reserved query allocator
   */
  public FragmentTicket newFragmentTicket(PlanFragment fragment) {
    final QueryId queryId = fragment.getHandle().getQueryId();
    final int majorFragmentId = fragment.getHandle().getMajorFragmentId();
    final WorkloadClass workloadClass = fragment.getPriority().getWorkloadClass();
    final long maxAllocation = fragment.getContext().getQueryMaxAllocation();

    QueryTicket queryTicket = getOrCreateQueryTicket(queryId, workloadClass, maxAllocation, fragment.getForeman(), fragment.getAssignment());
    // Note: applying query limit to the phase, as that doesn't add any additional restrictions. If an when we have
    // phase limits on the plan fragment, we could apply them here.
    PhaseTicket phaseTicket = queryTicket.getOrCreatePhaseTicket(majorFragmentId, maxAllocation);
    return new FragmentTicket(phaseTicket);
  }

  /**
   * @return all the active query tickets
   */
  Collection<QueryTicket> getActiveQueryTickets() {
    return ImmutableList.copyOf(queryTickets.values());
  }

  /**
   * Get the allocator that corresponds to the appropriate workload
   */
  private BufferAllocator getWorkloadAllocator(WorkloadClass workloadClass) {
    switch (workloadClass) {
      case BACKGROUND:
        return backgroundAllocator;
      case GENERAL:
        return generalAllocator;
      case NRT:
        return nrtAllocator;
      default: // REALTIME priority is not handled for now
        throw new IllegalStateException("Unknown work class: " + workloadClass);
    }
  }

  @Override
  public void close() throws Exception {
    AutoCloseables.close(nrtAllocator, backgroundAllocator, generalAllocator);
  }

  public class FragmentTicket implements AutoCloseable {
    private PhaseTicket phaseTicket;
    private boolean closed;

    private FragmentTicket(PhaseTicket phaseTicket) {
      this.phaseTicket = Preconditions.checkNotNull(phaseTicket, "PhaseTicket should not be null");
      phaseTicket.reserve();
    }

    public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
      return phaseTicket.getAllocator().newChildAllocator(name, initReservation, maxAllocation);
    }

    @Override
    public void close() throws Exception {
      Preconditions.checkState(!closed, "Trying to close FragmentTicket more than once");
      closed = true;

      if (phaseTicket.release()) {
        // NB: The query ticket removes itself from the queries clerk when its last phase ticket is removed
        phaseTicket.getQueryTicket().removePhaseTicket(phaseTicket);
      }
    }
  }
}
