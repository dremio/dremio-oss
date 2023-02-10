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
package com.dremio.sabot.exec.heap;

import java.lang.management.MemoryPoolMXBean;

/**
 * Continuously monitors a heap memory pool that has reached a collection usage threshold post GC.
 * Removes the entry once the heap memory pool usage goes below the threshold.
 */
final class HeapPoolMonitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HeapPoolMonitor.class);
  private static final long GB = 1024 * 1024 * 1024;

  private final MemoryPoolMXBean pool;
  private final int sizeFactor;
  private int notificationCount;
  private int checkCount;
  private long currentLowMemThreshold;
  private long currentVeryLowMemThreshold;
  private long backToNormalThreshold;

  HeapPoolMonitor(int currentLowMemThresholdPercent, MemoryPoolMXBean pool, boolean thresholdCrossed) {
    this.pool = pool;
    final long max = pool.getUsage().getMax();
    this.sizeFactor = Math.max((int) (max / GB), 2);
    computeThresholds(currentLowMemThresholdPercent, max);
    this.notificationCount = (thresholdCrossed) ? 1 : 0;
    this.checkCount = 0;
    logger.info("Memory pool `{}` added for low memory monitoring. " +
        "Low mem Threshold Range is {} to {} with size factor {}", pool.getName(), currentLowMemThreshold,
      currentVeryLowMemThreshold, sizeFactor);
  }

  int getSizeFactor() {
    return sizeFactor;
  }

  /**
   * Checks and returns the memory availability state of this pool.
   * <p>
   * Assumed to be protected under pool lock
   * </p>
   * @return Available memory state of this pool
   */
  MemoryState checkMemoryState(MemoryState currentState) {
    checkCount++;
    final long currentUsed = pool.getUsage().getUsed();
    final long lowMemThresholdToCheck = currentState.getSeverity() > 0 ? backToNormalThreshold : currentLowMemThreshold;
    if (currentUsed >= currentVeryLowMemThreshold) {
      return (notificationCount > 0 || checkCount > 2) ? MemoryState.VERY_LOW : MemoryState.LOW;
    }
    if (currentUsed >= lowMemThresholdToCheck) {
      return (notificationCount > 0 || checkCount > 2) ? MemoryState.LOW : MemoryState.BORDER;
    }
    logger.debug("Memory pool `{}` going back to Normal state", pool.getName());
    checkCount = 0;
    notificationCount = 0;
    return MemoryState.NORMAL;
  }

  /**
   * Increments when notified again while the pool is in this state. Indicates memory pressure.
   * <p>
   * Assumed to be protected under pool lock
   * </p>
   */
  void handleThresholdCrossedNotification() {
    this.notificationCount++;
  }

  void changeLowMemThresholdPercentage(int newLowMemThresholdPercentage) {
    computeThresholds(newLowMemThresholdPercentage, pool.getUsage().getMax());
  }

  private void computeThresholds(int currentLowMemThresholdPercent, long max) {
    this.currentLowMemThreshold = (max * currentLowMemThresholdPercent) / 100;
    this.backToNormalThreshold = (max * (currentLowMemThresholdPercent - 5)) / 100;
    int veryLowPercent = currentLowMemThresholdPercent + 10;
    if (veryLowPercent > 95) {
      veryLowPercent = 95;
    }
    this.currentVeryLowMemThreshold = (max * veryLowPercent) / 100;
  }
}
