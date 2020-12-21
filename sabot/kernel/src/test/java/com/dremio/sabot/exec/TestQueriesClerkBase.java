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

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.arrow.memory.AllocationListener;
import org.apache.arrow.memory.AllocationReservation;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.junit.After;
import org.junit.Before;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.planner.fragment.PlanFragmentFull;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;

/**
 * Base class for TestQueriesClerk
 *
 * Owns the test allocator, the root test allocator, rules for creation/destruction of the root test allocator, etc.
 */
public class TestQueriesClerkBase {
  protected BufferAllocator mockedRootAlloc;  // a mock-up of a buffer allocator, useful only for counting children

  @Before
  public void setup() {
    assertEquals(0, getNumAllocators());
    mockedRootAlloc = new TestAllocator(null, "root", Long.MAX_VALUE);
  }

  @After
  public void teardown() throws Exception {
    AutoCloseables.close(mockedRootAlloc);
    assertEquals(0, getNumAllocators());
  }

  // Bare-bones plan fragment: only contains the major and minor fragment IDs
  protected PlanFragmentFull getDummyPlan(UserBitShared.QueryId queryId, int majorFragmentId, int minorFragmentId){
    ExecProtos.FragmentHandle handle = ExecProtos.FragmentHandle
      .newBuilder()
      .setQueryId(queryId)
      .setMajorFragmentId(majorFragmentId)
      .build();

    CoordExecRPC.FragmentPriority priority = CoordExecRPC.FragmentPriority
      .newBuilder()
      .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL)
      .build();

    return new PlanFragmentFull(
      CoordExecRPC.PlanFragmentMajor.newBuilder()
        .setHandle(handle)
        .setPriority(priority)
        .build(),
      CoordExecRPC.PlanFragmentMinor.newBuilder()
        .setMinorFragmentId(minorFragmentId)
        .build());
  }

  protected CoordExecRPC.SchedulingInfo getDummySchedulingInfo() {
    return CoordExecRPC.SchedulingInfo
      .newBuilder()
      .setQueueId("unused")
      .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL)
      .build();
  }

  private static int numAllocators = 0;
  protected static int getNumAllocators() {
    return numAllocators;
  }

  /**
   * Count the number of major fragments running
   * (Note: phases are easy to count. Fragments, on the other hand, are not -- fragments are not tracked within the
   * phase ticket, so counting them is impossible)
   */
  protected void assertLivePhasesCount(QueriesClerk clerk, int expectedValue) {
    List<QueryTicket> queryTickets = new ArrayList<>();
    for (WorkloadTicket workloadTicket : clerk.getWorkloadTickets()) {
      queryTickets.addAll(workloadTicket.getActiveQueryTickets());
    }
    int numPhases = 0;
    for(QueryTicket c : queryTickets) {
      assertTrue(String.format("Fewer refcounts (%d) than active phase tickets (%d)", c.getNumChildren(), c.getActivePhaseTickets().size()),
        c.getNumChildren() >= c.getActivePhaseTickets().size());
      numPhases += c.getActivePhaseTickets().size();
    }
    assertEquals(expectedValue, numPhases);
  }

  protected QueriesClerk makeClerk(WorkloadTicketDepot ticketDepot) {
    ExecToCoordTunnelCreator mockTunnelCreator = mock(ExecToCoordTunnelCreator.class);
    ExecToCoordTunnel mockTunnel = mock(ExecToCoordTunnel.class);
    return new QueriesClerk(ticketDepot);
  }

  public final class TestAllocator implements BufferAllocator {
    private BufferAllocator parent;
    private String name;
    private long limit;

    private TestAllocator(BufferAllocator parent, String name, long limit) {
      this.parent = parent;
      this.name = name;
      this.limit = limit;
      numAllocators++;
    }

    public BufferAllocator newChildAllocator(String childName, long dummy1, long limit) {
      return newChildAllocator(childName, null, dummy1, limit);
    }
    public BufferAllocator newChildAllocator(String childName, AllocationListener listener, long dummy1, long limit) {
      return new TestAllocator(this, childName, limit);
    }
    public void close() {
      numAllocators--;
    }

    // NB: None of the functions below are implemented
    public ArrowBuf buffer(long var1) {
      throw new UnsupportedOperationException();
    }
    public ArrowBuf buffer(long var1, BufferManager var2) {
      throw new UnsupportedOperationException();
    }
    public long getAllocatedMemory() {
      throw new UnsupportedOperationException();
    }
    public long getLimit() {
      return limit;
    }
    public long getInitReservation() {
      throw new UnsupportedOperationException();
    }
    public void setLimit(long var1) {
      throw new UnsupportedOperationException();
    }
    public long getPeakMemoryAllocation() {
      return 0;
    }
    public long getHeadroom() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BufferAllocator getParentAllocator() {
      throw new UnsupportedOperationException();
    }

    @Override
    public Collection<BufferAllocator> getChildAllocators() {
      throw new UnsupportedOperationException();
    }

    public AllocationReservation newReservation() {
      throw new UnsupportedOperationException();
    }
    public ArrowBuf getEmpty() {
      throw new UnsupportedOperationException();
    }
    public String getName() {
      return name;
    }
    public boolean isOverLimit() {
      throw new UnsupportedOperationException();
    }
    public String toVerboseString() {
      throw new UnsupportedOperationException();
    }
    public void assertOpen() {
      throw new UnsupportedOperationException();
    }
    @Override
    public AllocationListener getListener() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void releaseBytes(long size) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean forceAllocate(long size) {
      throw new UnsupportedOperationException();

    }

    @Override
    public BufferAllocator getRoot() {
      return this;
    }
  }

  /**
   * Used as a callback to the clerk's "build and start query" -- whose only job in this test is to actually
   * get the query ticket
   */
  public final class QueryTicketGetter implements QueryStarter, AutoCloseable {
    QueryTicket obtainedTicket = null;

    @Override
    public void buildAndStartQuery(QueryTicket ticket) {
      obtainedTicket = ticket;
    }

    @Override
    public void unableToBuildQuery(Exception e) {
      fail(String.format("Unable to build query. Received exception: %s", e.toString()));
    }

    @Override
    public void close() {
      if (obtainedTicket != null) {
        obtainedTicket.release();
      }
    }

    public QueryTicket getObtainedTicket() {
      return obtainedTicket;
    }
  }
}

