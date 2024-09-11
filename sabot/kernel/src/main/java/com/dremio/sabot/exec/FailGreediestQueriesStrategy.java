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
import com.google.common.base.Joiner;
import java.lang.management.MemoryPoolMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Cancels queries consuming up to the specified percentage of the total consumed memory */
public class FailGreediestQueriesStrategy extends AbstractHeapClawBackStrategy
    implements HeapLowMemListener, DumpUsageObserver {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(FailGreediestQueriesStrategy.class);
  private static final long LOW_MEM_LOG_GAP = TimeUnit.MINUTES.toMillis(5);

  private final long cancelPercentage;
  private long lastLoggedOnLowMemMillis = 0;

  public FailGreediestQueriesStrategy(
      FragmentExecutors fragmentExecutors, QueriesClerk queriesClerk, long cancelPercentage) {
    super(fragmentExecutors, queriesClerk);
    this.cancelPercentage = cancelPercentage;
  }

  @Override
  public void clawBack(HeapClawBackContext clawBackContext) {
    final String activeQueryDump = fragmentExecutors.activeQueriesToCsv(queriesClerk);
    logger.info(
        "Dumping current fragment executor state as heap monitor is about to kill some queries: {}{}",
        System.lineSeparator(),
        activeQueryDump);
    List<ActiveQuery> activeQueries = getSortedActiveQueries();

    if (activeQueries.isEmpty()) {
      logger.info("No active queries, nothing to fail");
      return;
    }

    // Find the total memory used (we assume that the heap usage is proportional to the direct
    // memory).
    long totalUsed = activeQueries.stream().mapToLong(x -> x.directMemoryUsed).reduce(0, Long::sum);

    // Collect queries amount to cancelPercentage% of the total usage (at least 1 query).
    List<QueryId> queriesToCancel = new ArrayList<>();
    final long memoryUsageCancellationThreshold = (totalUsed * cancelPercentage) / 100;
    long cumulativeMemoryUsageByQueries = 0;

    for (ActiveQuery activeQuery : activeQueries) {
      queriesToCancel.add(activeQuery.queryId);
      cumulativeMemoryUsageByQueries += activeQuery.directMemoryUsed;
      if (cumulativeMemoryUsageByQueries >= memoryUsageCancellationThreshold) {
        break;
      }
    }

    logger.info(
        "Canceling {} queries that together consume {}% of memory (cancellation of up to {}% of memory was requested by {}):\n{}",
        queriesToCancel.size(),
        Math.round(((float) cumulativeMemoryUsageByQueries / totalUsed) * 100),
        this.cancelPercentage,
        clawBackContext.getTrigger(),
        Joiner.on("\n").join(queriesToCancel));

    failQueries(queriesToCancel, clawBackContext);
  }

  @Override
  public void handleMemNotification(boolean collectionThresholdCrossed, MemoryPoolMXBean pool) {}

  @Override
  public void handleUsageCrossedNotification() {
    final long current = System.currentTimeMillis();
    if (lastLoggedOnLowMemMillis == 0 || (current - lastLoggedOnLowMemMillis) > LOW_MEM_LOG_GAP) {
      final String activeQueryDump = fragmentExecutors.activeQueriesToCsv(queriesClerk);
      logger.info(
          "Dumping current fragment executor state as heap usage has crossed low mem threshold: {}{}",
          System.lineSeparator(),
          activeQueryDump);
      lastLoggedOnLowMemMillis = current;
    }
  }

  @Override
  public void changeLowMemOptions(long newThresholdPercentage, long newAggressiveWidthLowerBound) {
    // nothing to do
  }

  @Override
  public void dumpUsageData() {
    final String activeQueryDump = fragmentExecutors.activeQueriesToCsv(queriesClerk);
    logger.info(
        "Dumping current fragment executor state on user request: {}{}",
        System.lineSeparator(),
        activeQueryDump);
  }
}
