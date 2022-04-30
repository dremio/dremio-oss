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

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.arrow.memory.BufferAllocator;

import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.google.common.collect.ImmutableList;

/**
 *  Manages the phase (major fragment) level allocator. Allows for reporting of phase-level stats to the coordinator.<br>
 *
 *  A PhaseTicket is created for each phase (major fragment) of a query on an executor node. The PhaseTicket tracks the
 *  allocator used for this phase. It contains a phase reporter that's used to report the status of this phase of this
 *  query on this node to the coordinator
 *
 *  PhaseTickets are issued by the QueriesClerk given a QueryTicket. In turn, given a PhaseTicket, the QueriesClerk can
 *  issue a FragmentTicket for any one (minor) fragment of this query
 *
 *  The PhaseTicket tracks the child FragmentTickets. When the last FragmentTicket is closed, the PhaseTicket closes the
 *  phase-level allocator. Any further operations on the phase-level allocator will throw an {@link IllegalStateException}
 */
public class PhaseTicket extends TicketWithChildren {
  private final QueryTicket queryTicket;
  private final int majorFragmentId;
  private final int phaseWeight;
  private final Set<FragmentTicket> fragmentTickets = ConcurrentHashMap.newKeySet();

  public PhaseTicket(QueryTicket queryTicket, int majorFragmentId, BufferAllocator allocator, int phaseWeight) {
    super(allocator);
    this.queryTicket = queryTicket;
    this.majorFragmentId = majorFragmentId;
    this.phaseWeight = phaseWeight;
  }

  public int getMajorFragmentId() {
    return majorFragmentId;
  }

  public QueryTicket getQueryTicket() {
    return queryTicket;
  }

  public void reserve(FragmentTicket fragmentTicket) {
    fragmentTickets.add(fragmentTicket);
    super.reserve();
  }

  public boolean release(FragmentTicket fragmentTicket) {
    fragmentTickets.remove(fragmentTicket);
    return super.release();
  }

  public Collection<FragmentTicket> getFragmentTickets() {
    return ImmutableList.copyOf(fragmentTickets);
  }

  /**
   * Return the status of the query's phase tracked by this ticket, on this node.
   */
  NodePhaseStatus getStatus() {
    return NodePhaseStatus.newBuilder()
      .setMajorFragmentId(majorFragmentId)
      .setMaxMemoryUsed(getAllocator().getPeakMemoryAllocation())
      .build();
  }

  public int getPhaseWeight() {
    return phaseWeight;
  }
}
