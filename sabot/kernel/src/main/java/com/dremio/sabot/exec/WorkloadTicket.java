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
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.SchedulingGroup;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

/**
 * Manages the workload level allocator, and eventually the workload scheduling group
 *
 * A WorkloadTicket is created for each queue on the executor node. They are created only when queues are established.
 *
 * The WorkloadTicket issues {@link QueryTicket}s. The WorkloadTicket tracks the child {@link QueryTicket}s.
 *
 * Permanent references to the WorkloadTicket are stored in the {@link WorkloadTicketDepot}.
 * A WorkloadTicket is destroyed when two conditions are met: all of its {@link QueryTicket}s have completed *and*
 * the {@link WorkloadTicketDepot} no longer stores a permanent reference to the WorkloadTicket.
 */
public class WorkloadTicket extends TicketWithChildren {
  protected final ConcurrentMap<QueryId, QueryTicket> queryTickets = Maps.newConcurrentMap();

  private SchedulingGroup<AsyncTaskWrapper> schedulingGroup;

  /**
   * Create a WorkloadTicket
   * @param allocator The allocator for this workload.
   */
  public WorkloadTicket(final BufferAllocator allocator, final SchedulingGroup<AsyncTaskWrapper> schedulingGroup) {
    super(allocator);
    setSchedulingGroup(schedulingGroup);
  }

  protected void setSchedulingGroup(SchedulingGroup<AsyncTaskWrapper> schedulingGroup) {
    this.schedulingGroup = Preconditions.checkNotNull(schedulingGroup, "scheduling group required");
  }

  /**
   * Obtains a query ticket (creating one if one hasn't already been created), and invokes the callback with
   * this ticket
   * Multi-thread safe
   */
  public void buildAndStartQuery(final QueryId queryId,
                                 final long maxAllocation,
                                 final CoordinationProtos.NodeEndpoint foreman,
                                 final CoordinationProtos.NodeEndpoint assignment,
                                 final ExecToCoordTunnelCreator tunnelCreator,
                                 final QueryStarter queryStarter) {
    QueryTicket queryTicket = queryTickets.get(queryId);
    if (queryTicket == null) {
      final BufferAllocator queryAllocator = makeQueryAllocator(getAllocator(), queryId, maxAllocation);
      queryTicket = new QueryTicket(this, queryId, queryAllocator, foreman, assignment, tunnelCreator, 0L);
      QueryTicket insertedTicket = queryTickets.putIfAbsent(queryId, queryTicket);
      if (insertedTicket == null) {
        this.reserve();
      } else {
        // Race condition: another user managed to insert a query ticket. Let's close ours and use theirs
        AutoCloseables.closeNoChecked(queryTicket);  // NB: closing the ticket will close the query allocator
        queryTicket = insertedTicket;
      }
    }
    queryTicket.reserve();
    queryStarter.buildAndStartQuery(queryTicket);
  }

  /**
   * Remove a query ticket from this workload ticket
   * <p>
   * Multi-thread safe
   */
  public void removeQueryTicket(QueryTicket queryTicket) throws Exception {
    final QueryId queryId = queryTicket.getQueryId();
    final QueryTicket removedQueryTicket = queryTickets.remove(queryId);
    Preconditions.checkState(removedQueryTicket == queryTicket,
      "closed query ticket was not found in the query tickets' map");
    try {
      AutoCloseables.close(queryTicket);
    } finally {
      if (this.release()) {
        AutoCloseables.close(this);
      }
    }
  }

  /**
   * Returns the maximum allocation for any queries running in this workload
   */
  public long getChildMaxAllocation(long proposedMaxAllocation) {
    return proposedMaxAllocation;
  }

  public SchedulingGroup<AsyncTaskWrapper> getSchedulingGroup() {
    return schedulingGroup;
  }

  /**
   * Create a query allocator for usage with the given query
   */
  protected BufferAllocator makeQueryAllocator(BufferAllocator parent, QueryId queryId, long maxAllocation) {
    return parent.newChildAllocator("query-" + QueryIdHelper.getQueryId(queryId), 0, maxAllocation);
  }

  /**
   * @return all the active query tickets
   */
  Collection<QueryTicket> getActiveQueryTickets() {
    return ImmutableList.copyOf(queryTickets.values());
  }
}
