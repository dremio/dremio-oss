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

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;

/**
 *  Manages the query level allocator and potentially the query scheduling group. Allows for reporting of query-level
 *  stats to the coordinator.<br>
 *
 *  A QueryTicket is created for each query that executes on an executor node. The QueryTicket tracks the allocator
 *  used for this query. It contains a query reporter that's used to report the status of this query on this node to
 *  the coordinator
 *
 *  QueryTickets are issued by the {@link WorkloadTicket}. Given a QueryTicket, the {@link QueriesClerk} can issue a
 *  {@link PhaseTicket} for any one phase of this query
 *
 *  The QueryTicket tracks the child {@link PhaseTicket}s. When the last {@link PhaseTicket} is closed, the QueryTicket
 *  closes the query-level allocator. Any further operations on the query-level allocator will throw an
 *  {@link IllegalStateException}
 */
public class QueryTicket extends TicketWithChildren {
  private final WorkloadTicket workloadTicket;
  private final QueryId queryId;
  private final NodeEndpoint foreman;
  private final NodeEndpoint assignment;
  private final ExecToCoordTunnelCreator tunnelCreator;
  private final ConcurrentMap<Integer, PhaseTicket> phaseTickets = Maps.newConcurrentMap();
  private final Collection<NodePhaseStatus> completed = Queues.newConcurrentLinkedQueue();
  private final long enqueuedTime;

  public QueryTicket(WorkloadTicket workloadTicket, QueryId queryId, BufferAllocator allocator, NodeEndpoint foreman,
                     NodeEndpoint assignment, ExecToCoordTunnelCreator tunnelCreator, long enqueuedTime) {
    super(allocator);
    this.workloadTicket = workloadTicket;
    this.queryId = Preconditions.checkNotNull(queryId, "queryId cannot be null");
    this.foreman = foreman;
    this.assignment = assignment;
    this.tunnelCreator = tunnelCreator;
    this.enqueuedTime = enqueuedTime;
  }

  public QueryId getQueryId() {
    return queryId;
  }

  public NodeEndpoint getForeman() {
    return foreman;
  }

  public NodeEndpoint getAssignment() {
    return assignment;
  }

  public long getEnqueuedTime() {
    return enqueuedTime;
  }

  /**
   * Creates a phase ticket (along with a phase-level allocator) for a given phase (major fragment) of this query, if
   * one has not already been created. The created phase ticket is tracked by this query ticket.
   *
   * Multi-thread safe`
   */
  public PhaseTicket getOrCreatePhaseTicket(int majorFragmentId, long maxAllocation) {
    PhaseTicket phaseTicket = phaseTickets.get(majorFragmentId);
    if (phaseTicket == null) {
      final BufferAllocator phaseAllocator = getAllocator().newChildAllocator("phase-" + majorFragmentId, 0, maxAllocation);
      phaseTicket = new PhaseTicket(this, majorFragmentId, phaseAllocator);
      PhaseTicket insertedTicket = phaseTickets.putIfAbsent(majorFragmentId, phaseTicket);
      if (insertedTicket == null) {
        this.reserve();
      } else {
        // Race condition: another user managed to insert a phase ticket. Let's close ours and use theirs
        Preconditions.checkState(insertedTicket != phaseTicket);
        try {
          AutoCloseables.close(phaseTicket);  // NB: closing the ticket will close the phaseAllocator
        } catch (Exception e) {
          // Ignored
        }
        phaseTicket = insertedTicket;
      }
    }
    return phaseTicket;
  }

  /**
   * Remove a phase ticket from this query ticket. When the last phase ticket is removed, this query ticket is
   * removed from the queries clerk, and a node query status is sent to the coordinator
   *
   * Multi-thread safe
   */
  public void removePhaseTicket(PhaseTicket phaseTicket) throws Exception{
    final NodePhaseStatus finalStatus = phaseTicket.getStatus();

    final PhaseTicket removedPhaseTicket = phaseTickets.remove(phaseTicket.getMajorFragmentId());
    Preconditions.checkState(removedPhaseTicket == phaseTicket,
      "closed phase ticket was not found in the phase tickets' map");
    try {
      AutoCloseables.close(phaseTicket);

      completed.add(finalStatus);
    } finally {
      if (this.release()) {
        NodeQueryStatus finalQueryStatus = getStatus();
        workloadTicket.removeQueryTicket(this);
        // NB: not waiting for an ack. Status report is on a best effort basis
        tunnelCreator.getTunnel(getForeman()).sendNodeQueryStatus(finalQueryStatus);
      }
    }
  }

  /**
   * @return all the active phase tickets for this query
   */
  Collection<PhaseTicket> getActivePhaseTickets() {
    return ImmutableList.copyOf(phaseTickets.values());
  }

  /**
   * Return the per-node query status for the query tracked by this ticket
   */
  NodeQueryStatus getStatus() {
    final NodeQueryStatus.Builder b = NodeQueryStatus.newBuilder()
      .setId(queryId)
      .setEndpoint(assignment)
      .setMaxMemoryUsed(getAllocator().getPeakMemoryAllocation())
      .setTimeEnqueuedBeforeSubmitMs(getEnqueuedTime());

    for (NodePhaseStatus nodePhaseStatus : completed) {
      b.addPhaseStatus(nodePhaseStatus);
    }
    for (PhaseTicket phaseTicket : getActivePhaseTickets()) {
      b.addPhaseStatus(phaseTicket.getStatus());
    }
    return b.build();
  }

  public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
    return workloadTicket.getSchedulingGroup();
  }

}
