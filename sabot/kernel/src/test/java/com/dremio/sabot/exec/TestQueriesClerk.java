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

import static com.dremio.sabot.task.single.DedicatedTaskPool.DUMMY_GROUP_MANAGER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.Test;

import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.sabot.task.AsyncTaskWrapper;
import com.dremio.sabot.task.TaskManager;
import com.dremio.sabot.task.single.DedicatedTaskPool;

/**
 * Unit test of QueriesClerk
 */
public class TestQueriesClerk extends TestQueriesClerkBase {

  /**
   * Tests the functionality of a queries clerk:
   * - allocators are created hierarchically;
   * - parent allocator is closed when all the child allocators are closed
   */
  @Test
  public void testQueriesClerk() throws Exception{
    WorkloadTicketDepot ticketDepot = new WorkloadTicketDepot(mockedRootAlloc, mock(SabotConfig.class), DUMMY_GROUP_MANAGER);
    QueriesClerk clerk = makeClerk(ticketDepot);
    assertLivePhasesCount(clerk, 0);
    int baseNumAllocators = getNumAllocators();

    UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder().setPart1(12).setPart2(23).build();
    QueryTicketGetter qtg1 = new QueryTicketGetter();
    clerk.buildAndStartQuery(getDummyPlan(queryId,1,0), getDummySchedulingInfo(), qtg1);
    QueryTicket queryTicket = qtg1.getObtainedTicket();
    assertNotNull(queryTicket);
    assertLivePhasesCount(clerk, 0);  // no phase- or fragment- items created yet
    assertEquals(baseNumAllocators + 1, getNumAllocators());  // new query-level allocator

    QueriesClerk.FragmentTicket ticket10 = clerk
      .newFragmentTicket(queryTicket, getDummyPlan(queryId,1,0), getDummySchedulingInfo());
    assertLivePhasesCount(clerk, 1);  // 1 for the phase-level allocator for major fragment 1
    assertEquals(baseNumAllocators + 2, getNumAllocators());  // new query- and phase-level allocators
    QueriesClerk.FragmentTicket ticket11 = clerk
      .newFragmentTicket(queryTicket, getDummyPlan(queryId,1,1), getDummySchedulingInfo());
    assertLivePhasesCount(clerk, 1);  // stays 1, as the ticket is for the same major fragment
    assertEquals(baseNumAllocators + 2, getNumAllocators());
    QueriesClerk.FragmentTicket ticket20 = clerk
      .newFragmentTicket(queryTicket, getDummyPlan(queryId,2,0), getDummySchedulingInfo());
    assertLivePhasesCount(clerk, 2);  // new phase-level allocator, for major fragment 2
    assertEquals(baseNumAllocators + 3, getNumAllocators());  // new phase-level allocator

    qtg1.close();
    assertEquals(baseNumAllocators + 3, getNumAllocators());  // fragment tickets still open, no change in allocators

    ticket10.close();
    assertLivePhasesCount(clerk, 2);  // stays two: each of the major fragments now has 1 child each
    assertEquals(baseNumAllocators + 3, getNumAllocators());
    ticket20.close();
    assertLivePhasesCount(clerk, 1);  // only major fragment 1 remains
    assertEquals(baseNumAllocators + 2, getNumAllocators());  // query-level allocator and phase-1 allocator
    ticket11.close();
    // Last ticket in the query was closed. Both the query-level allocator and the phase-level allocator for phase 1 are now closed
    assertLivePhasesCount(clerk, 0);
    assertEquals(baseNumAllocators, getNumAllocators());

    AutoCloseables.close(ticketDepot);
  }

}
