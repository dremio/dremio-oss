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
import java.util.HashMap;
import java.util.Map;

/**
 * Monitors Java memory pool(s) that has both collection usage and usage threshold notifications for low memory
 * conditions.
 * <p>
 * Note that the G1GC has only a single such pool which is the 'G1 Old Gen' pool. However, this implementation
 * supports multiple such pools in order to make this implementation less tied to Java releases.
 * The methods are not thread safe and it is assumed that the controller calls these method after holding the pool
 * lock.
 * </p>
 */
final class CompositePoolMonitor {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CompositePoolMonitor.class);

  private final Map<String, HeapPoolMonitor> poolMonitors;
  private MemoryState currentState;
  private HeapPoolMonitor firstPoolMonitor;

  CompositePoolMonitor() {
    this.poolMonitors = new HashMap<>();
    this.currentState = MemoryState.INACTIVE;
    this.firstPoolMonitor = null;
  }

  int getSizeFactor() {
    return firstPoolMonitor == null ? 2 : firstPoolMonitor.getSizeFactor();
  }

  void activate() {
    currentState = MemoryState.NORMAL;
  }

  void deactivate() {
    currentState = MemoryState.INACTIVE;
  }

  void processPoolEvent(MemoryPoolMXBean pool, int currentLowMemThresholdPercentage, boolean thresholdCrossed) {
    HeapPoolMonitor hpm = poolMonitors.compute(pool.getName(), (k, v) -> {
      if (v == null) {
        return new HeapPoolMonitor(currentLowMemThresholdPercentage, pool, thresholdCrossed);
      } else {
        if (thresholdCrossed) {
          v.handleThresholdCrossedNotification();
        }
        return v;
      }
    });
    if (firstPoolMonitor == null) {
      firstPoolMonitor = hpm;
    }
  }

  /**
   * Checks and returns the memory availability state of this pool. State of the composite pool is the
   * state of the underlying pool it manages.
   * <p>
   * Assumed to be protected under pool lock
   * </p>
   *
   * @return Available memory state of this pool
   */
  public MemoryState checkMemoryState() {
    if (currentState.equals(MemoryState.INACTIVE)) {
      return MemoryState.INACTIVE;
    }
    MemoryState current = MemoryState.NORMAL;
    for (HeapPoolMonitor p : poolMonitors.values()) {
      MemoryState next = p.checkMemoryState(currentState);
      if (next.getSeverity() > current.getSeverity()) {
        current = next;
      }
    }
    currentState = current;
    logger.debug("Composite current state is `{}`", currentState);
    return currentState;
  }

  public MemoryState getCurrentMemoryState() {
    return currentState;
  }

  public void changeThreshold(int newLowMemThresholdPercentage) {
    for (HeapPoolMonitor p : poolMonitors.values()) {
      p.changeLowMemThresholdPercentage(newLowMemThresholdPercentage);
    }
  }
}
