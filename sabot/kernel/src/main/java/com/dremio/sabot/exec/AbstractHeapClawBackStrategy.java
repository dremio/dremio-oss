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

import com.dremio.exec.proto.UserBitShared.QueryId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/** Abstract strategy for reducing heap usage. */
public abstract class AbstractHeapClawBackStrategy implements HeapClawBackStrategy {
  public static final String FAIL_CONTEXT = "Query canceled by executor heap monitor";

  protected FragmentExecutors fragmentExecutors;
  protected QueriesClerk queriesClerk;

  public AbstractHeapClawBackStrategy(
      FragmentExecutors fragmentExecutors, QueriesClerk queriesClerk) {
    this.queriesClerk = queriesClerk;
    this.fragmentExecutors = fragmentExecutors;
  }

  public class ActiveQuery {
    QueryId queryId;
    long directMemoryUsed;

    public ActiveQuery(QueryId queryId, long directMemoryUsed) {
      this.queryId = queryId;
      this.directMemoryUsed = directMemoryUsed;
    }
  }

  /**
   * Get the list of active queries, sorted by the used memory.
   *
   * @return list of active queries.
   */
  protected List<ActiveQuery> getSortedActiveQueries() {
    List<ActiveQuery> queryList = new ArrayList<>();

    for (final WorkloadTicket workloadTicket : queriesClerk.getWorkloadTickets()) {
      for (final QueryTicket queryTicket : workloadTicket.getActiveQueryTickets()) {
        queryList.add(
            new ActiveQuery(
                queryTicket.getQueryId(), queryTicket.getAllocator().getAllocatedMemory()));
      }
    }

    // sort in descending order of memory usage.
    queryList.sort(Comparator.comparingLong(x -> -x.directMemoryUsed));
    return queryList;
  }

  /**
   * Fail the queries in the input list.
   *
   * @param queries list of queries to be cancelled.
   */
  protected void failQueries(
      List<QueryId> queries, Throwable throwable, String failContext, String extraDebugInfo) {
    for (QueryId queryId : queries) {
      fragmentExecutors.failFragments(
          queryId, queriesClerk, throwable, failContext, extraDebugInfo);
    }
  }
}
