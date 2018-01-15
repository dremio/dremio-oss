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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;

import org.apache.arrow.memory.AllocationReservation;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.BufferManager;
import org.junit.Test;

import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.CoordExecRPC;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.exec.rpc.ExecToCoordTunnel;
import io.netty.buffer.ArrowBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Unit test of QueriesClerk
 */
public class TestQueriesClerk {

  // Bare-bones plan fragment: only contains the major and minor fragment IDs
  private CoordExecRPC.PlanFragment getDummyPlan(UserBitShared.QueryId queryId, int majorFragmentId, int minorFragmentId){
    ExecProtos.FragmentHandle handle = ExecProtos.FragmentHandle
      .newBuilder()
      .setQueryId(queryId)
      .setMajorFragmentId(majorFragmentId)
      .setMinorFragmentId(minorFragmentId)
      .build();

    CoordExecRPC.FragmentPriority priority = CoordExecRPC.FragmentPriority
      .newBuilder()
      .setWorkloadClass(UserBitShared.WorkloadClass.GENERAL)
      .build();

    return CoordExecRPC.PlanFragment
      .newBuilder()
      .setHandle(handle)
      .setPriority(priority)
      .build();
  }

  private void assertLiveCount(QueriesClerk clerk, int expectedValue) {
    Collection<QueryTicket> queryTickets = clerk.getActiveQueryTickets();
    // All tests are single-query
    assertTrue((queryTickets.size() == 1 && expectedValue > 0) || (queryTickets.size() == 0 && expectedValue == 0));
    for(QueryTicket c : queryTickets) {
      assertEquals(expectedValue, c.getNumChildren());
      assertEquals(expectedValue, c.getActivePhaseTickets().size());
    }
  }

  /**
   * Tests the functionality of a queries clerk:
   * - allocators are created hierarchically;
   * - parent allocator is closed when all the child allocators are closed
   */
  @Test
  public void testQueriesClerk() throws Exception{
    assertEquals(0, numAllocators);
    BufferAllocator rootAlloc = new TestAllocator(null, "root");

    ExecToCoordTunnelCreator mockTunnelCreator = mock(ExecToCoordTunnelCreator.class);
    ExecToCoordTunnel mockTunnel = mock(ExecToCoordTunnel.class);
    when(mockTunnelCreator.getTunnel(any(CoordinationProtos.NodeEndpoint.class))).thenReturn(mockTunnel);
    QueriesClerk clerk = new QueriesClerk(rootAlloc, mock(SabotConfig.class), mockTunnelCreator);
    assertLiveCount(clerk, 0);
    int baseNumAllocators = numAllocators;

    UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder().setPart1(12).setPart2(23).build();
    QueriesClerk.FragmentTicket ticket10 = clerk.newFragmentTicket(getDummyPlan(queryId,1,0));
    assertLiveCount(clerk, 1);  // 1 for the phase-level allocator for major fragment 1
    assertEquals(baseNumAllocators + 2, numAllocators);  // new query- and phase-level allocators
    QueriesClerk.FragmentTicket ticket11 = clerk.newFragmentTicket(getDummyPlan(queryId,1,1));
    assertLiveCount(clerk, 1);  // stays 1, as the ticket is for the same major fragment
    assertEquals(baseNumAllocators + 2, numAllocators);
    QueriesClerk.FragmentTicket ticket20 = clerk.newFragmentTicket(getDummyPlan(queryId,2,0));
    assertLiveCount(clerk, 2);  // new phase-level allocator, for major fragment 2
    assertEquals(baseNumAllocators + 3, numAllocators);  // new phase-level allocator

    ticket10.close();
    assertLiveCount(clerk, 2);  // stays two: each of the major fragments now has 1 child each
    assertEquals(baseNumAllocators + 3, numAllocators);
    ticket20.close();
    assertLiveCount(clerk, 1);  // only major fragment 1 remains
    assertEquals(baseNumAllocators + 2, numAllocators);  // query-level allocator and phase-1 allocator
    ticket11.close();
    // Last ticket in the query was closed. Both the query-level allocator and the phase-level allocator for phase 1 are now closed
    assertLiveCount(clerk, 0);
    assertEquals(baseNumAllocators, numAllocators);
  }

  private static int numAllocators = 0;

  private final class TestAllocator implements BufferAllocator {
    private BufferAllocator parent;
    private String name;

    TestAllocator(BufferAllocator parent, String name) {
      this.parent = parent;
      this.name = name;
      numAllocators++;
    }
    public BufferAllocator newChildAllocator(String childName, long dummy1, long dummy2) {
      return new TestAllocator(this, childName);
    }
    public void close() {
      numAllocators--;
    }

    // NB: None of the functions below are implemented
    public ArrowBuf buffer(int var1) {
      throw new UnsupportedOperationException();
    }
    public ArrowBuf buffer(int var1, BufferManager var2) {
      throw new UnsupportedOperationException();
    }
    public ByteBufAllocator getAsByteBufAllocator() {
      throw new UnsupportedOperationException();
    }
    public long getAllocatedMemory() {
      throw new UnsupportedOperationException();
    }
    public long getLimit() {
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
    public AllocationReservation newReservation() {
      throw new UnsupportedOperationException();
    }
    public ArrowBuf getEmpty() {
      throw new UnsupportedOperationException();
    }
    public String getName() {
      throw new UnsupportedOperationException();
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
  }
}
