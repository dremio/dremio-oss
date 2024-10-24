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
import com.dremio.common.VM;
import com.dremio.exec.planner.fragment.EndpointsIndex;
import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.exec.rpc.TunnelProvider;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Manages the query level allocator and potentially the query scheduling group. Allows for
 * reporting of query-level stats to the coordinator.<br>
 * A QueryTicket is created for each query that executes on an executor node. The QueryTicket tracks
 * the allocator used for this query. It contains a query reporter that's used to report the status
 * of this query on this node to the coordinator
 *
 * <p>QueryTickets are issued by the {@link WorkloadTicket}. Given a QueryTicket, the {@link
 * QueriesClerk} can issue a {@link PhaseTicket} for any one phase of this query
 *
 * <p>The QueryTicket tracks the child {@link PhaseTicket}s. When the last {@link PhaseTicket} is
 * closed, the QueryTicket closes the query-level allocator. Any further operations on the
 * query-level allocator will throw an {@link IllegalStateException}
 */
public class QueryTicket extends TicketWithChildren {
  public static final int MAX_EXPECTED_SIZE = 1000;

  private final WorkloadTicket workloadTicket;
  private final QueryId queryId;
  private final NodeEndpoint foreman;
  private final NodeEndpoint assignment;
  private final ConcurrentMap<Integer, PhaseTicket> phaseTickets = new ConcurrentHashMap<>();
  private final Collection<NodePhaseStatus> completed = new ConcurrentLinkedQueue<>();
  private final long enqueuedTime;
  private final SchedulingGroup<AsyncTaskWrapper> queryGroup;
  private TunnelProvider tunnelProvider;
  private EndpointsIndex endpointsIndex;
  private volatile NodeQueryStatus finalQueryStatus;
  private static int NUMBER_OF_CORES = VM.availableProcessors();

  public QueryTicket(
      WorkloadTicket workloadTicket,
      QueryId queryId,
      BufferAllocator allocator,
      NodeEndpoint foreman,
      NodeEndpoint assignment,
      long enqueuedTime,
      boolean useWeightBasedScheduling,
      int expectedNumTickets) {
    super(allocator);
    this.workloadTicket = workloadTicket;
    this.queryId = Preconditions.checkNotNull(queryId, "queryId cannot be null");
    this.foreman = foreman;
    this.assignment = assignment;
    this.enqueuedTime = enqueuedTime;
    final int queryWeight = Math.min(expectedNumTickets, MAX_EXPECTED_SIZE);
    this.queryGroup =
        this.workloadTicket
            .getSchedulingGroup()
            .addGroup((queryWeight <= 0) ? 1 : queryWeight, useWeightBasedScheduling);
    this.tunnelProvider = null;
    this.endpointsIndex = null;
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
   * Creates a phase ticket (along with a phase-level allocator) for a given phase (major fragment)
   * of this query, if one has not already been created. The created phase ticket is tracked by this
   * query ticket.
   *
   * <p>Multi-thread safe`
   */
  public PhaseTicket getOrCreatePhaseTicket(
      int majorFragmentId, long maxAllocation, int fragmentWeight) {
    return phaseTickets.compute(
        majorFragmentId,
        (k, v) -> {
          if (v == null) {
            final BufferAllocator phaseAllocator =
                getAllocator().newChildAllocator("phase-" + majorFragmentId, 0, maxAllocation);
            final PhaseTicket phaseTicket =
                new PhaseTicket(this, majorFragmentId, phaseAllocator, fragmentWeight);
            this.reserve();
            return phaseTicket;
          } else {
            return v;
          }
        });
  }

  /**
   * Remove a phase ticket from this query ticket. When the last phase ticket is removed, this query
   * ticket is removed from the queries clerk.
   *
   * <p>Multi-thread safe
   */
  public void removePhaseTicket(PhaseTicket phaseTicket) throws Exception {
    final NodePhaseStatus finalStatus = phaseTicket.getStatus();
    completed.add(finalStatus);

    final PhaseTicket removedPhaseTicket = phaseTickets.remove(phaseTicket.getMajorFragmentId());
    Preconditions.checkState(
        removedPhaseTicket == phaseTicket,
        "closed phase ticket was not found in the phase tickets' map");
    try {
      AutoCloseables.close(phaseTicket);
    } finally {
      if (this.release()) {
        finalQueryStatus = getStatus();
        workloadTicket.removeQueryTicket(this);
      }
    }
  }

  /**
   * @return all the active phase tickets for this query in descending order of their weights.
   */
  Collection<PhaseTicket> getActivePhaseTickets() {
    return phaseTickets.values().stream()
        .sorted(Comparator.comparingInt(PhaseTicket::getPhaseWeight).reversed())
        .collect(Collectors.toList());
  }

  /** Return the per-node query status for the query tracked by this ticket */
  NodeQueryStatus getStatus() {
    if (finalQueryStatus != null) {
      return finalQueryStatus;
    }

    final NodeQueryStatus.Builder b =
        NodeQueryStatus.newBuilder()
            .setId(queryId)
            .setEndpoint(assignment)
            .setMaxMemoryUsed(getAllocator().getPeakMemoryAllocation())
            .setTimeEnqueuedBeforeSubmitMs(getEnqueuedTime())
            .setNumberOfCores(NUMBER_OF_CORES);

    Set<Integer> addedPhases = new HashSet<>();
    for (NodePhaseStatus nodePhaseStatus : completed) {
      if (!addedPhases.contains(nodePhaseStatus.getMajorFragmentId())) {
        b.addPhaseStatus(nodePhaseStatus);
        addedPhases.add(nodePhaseStatus.getMajorFragmentId());
      }
    }
    for (PhaseTicket phaseTicket : getActivePhaseTickets()) {
      if (!addedPhases.contains(phaseTicket.getMajorFragmentId())) {
        b.addPhaseStatus(phaseTicket.getStatus());
        addedPhases.add(phaseTicket.getMajorFragmentId());
      }
    }
    // it's possible some phase switched from active -> completed in between the two
    // loops above. so, process the completed list again.
    for (NodePhaseStatus nodePhaseStatus : completed) {
      if (!addedPhases.contains(nodePhaseStatus.getMajorFragmentId())) {
        b.addPhaseStatus(nodePhaseStatus);
        addedPhases.add(nodePhaseStatus.getMajorFragmentId());
      }
    }
    return b.build();
  }

  public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
    return this.queryGroup;
  }

  public void setEndpointsAndTunnelProvider(
      EndpointsIndex endpointsIndex, TunnelProvider tunnelProvider) {
    this.tunnelProvider = tunnelProvider;
    this.endpointsIndex = endpointsIndex;
  }

  public TunnelProvider getTunnelProvider() {
    return this.tunnelProvider;
  }

  public EndpointsIndex getEndpointsIndex() {
    return this.endpointsIndex;
  }
}
