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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC.SchedulingInfo;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.google.common.base.Preconditions;

/**
 * Manages workload and query level allocators
 */
@ThreadSafe
public class QueriesClerk {

  private final ExecToCoordTunnelCreator tunnelCreator;
  private final WorkloadTicketDepot workloadTicketDepot;

  QueriesClerk(final WorkloadTicketDepot workloadTicketDepot, ExecToCoordTunnelCreator tunnelCreator) {
    this.tunnelCreator = tunnelCreator;
    this.workloadTicketDepot = workloadTicketDepot;
  }

  /**
   * Builds and starts a new query, if sufficient resources are available.
   * In case resources are not available immediately, the query will be started later, when resources become available
   */
  public void buildAndStartQuery(final PlanFragmentFull firstFragment, final SchedulingInfo schedulingInfo,
                                 final QueryStarter queryStarter) {
    final QueryId queryId = firstFragment.getHandle().getQueryId();

    // Note: The temporary reference count (released in the finally clause, below) is necessary to guard against races
    // between potential workload ticket modifications and this function (creation of fragments for queries on the workload)
    WorkloadTicket workloadTicket = workloadTicketDepot.getWorkloadTicket(schedulingInfo);
    try {
      final long queryMaxAllocation = workloadTicket.getChildMaxAllocation(firstFragment.getMajor().getContext().getQueryMaxAllocation());

      workloadTicket.buildAndStartQuery(queryId, queryMaxAllocation, firstFragment.getMajor().getForeman(), firstFragment.getMinor().getAssignment(),
        tunnelCreator, queryStarter);
    } finally {
      workloadTicket.release();
    }
  }

  /**
   * creates a fragment ticket for the passed-in fragment. May create a new query ticket if no query ticket is already
   * cached. Closing the ticket will return the reservation and eventually close the corresponding query ticket along with
   * its allocator
   *
   * @param queryTicket    the query ticket, obtained from the callback from {@link #buildAndStartQuery(PlanFragmentFull, SchedulingInfo, QueryStarter)}, above
   * @param fragment       fragment plan
   * @param schedulingInfo information about where should 'fragment' run
   * @return reserved query allocator
   */
  public FragmentTicket newFragmentTicket(final QueryTicket queryTicket, final PlanFragmentFull fragment, final SchedulingInfo schedulingInfo) {
    // Note: applying query limit to the phase, as that doesn't add any additional restrictions. If an when we have
    // phase limits on the plan fragment, we could apply them here.
    PhaseTicket phaseTicket = queryTicket
      .getOrCreatePhaseTicket(fragment.getHandle().getMajorFragmentId(), queryTicket.getAllocator().getLimit());
    return new FragmentTicket(phaseTicket, queryTicket.getSchedulingGroup());
  }

  /**
   * @return all the active query tickets
   */
  Collection<WorkloadTicket> getWorkloadTickets() {
    return workloadTicketDepot.getWorkloadTickets();
  }

  public class FragmentTicket implements AutoCloseable {
    private PhaseTicket phaseTicket;
    private final SchedulingGroup<AsyncTaskWrapper> schedulingGroup;
    private boolean closed;

    private FragmentTicket(PhaseTicket phaseTicket, SchedulingGroup<AsyncTaskWrapper> schedulingGroup) {
      this.phaseTicket = Preconditions.checkNotNull(phaseTicket, "PhaseTicket should not be null");
      this.schedulingGroup = Preconditions.checkNotNull(schedulingGroup, "Scheduling group required");
      phaseTicket.reserve();
    }

    public BufferAllocator newChildAllocator(String name, long initReservation, long maxAllocation) {
      return phaseTicket.getAllocator().newChildAllocator(name, initReservation, maxAllocation);
    }

    public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
      return schedulingGroup;
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
