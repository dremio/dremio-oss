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

import java.lang.management.MemoryPoolMXBean;

/**
 * Should be implemented by any dremio controller that requires to take action on a low memory
 * signal.
 */
public interface HeapLowMemListener {
  /**
   * Handle memory notifications from heap monitor. if {@code collectionThresholdCrossed} is true,
   * this indicates that collection usage threshold (post GC) has crossed. Otherwise, this just
   * indicates which pool has collection threshold configured.
   *
   * @param collectionThresholdCrossed whether collection usage threshold has crossed
   * @param pool which pool has collection usage threshold crossed
   */
  void handleMemNotification(boolean collectionThresholdCrossed, MemoryPoolMXBean pool);

  /**
   * Handle usage threshold notifications from monitor. Rather than indicating the pool, this
   * indicates that the usage (pre GC) has crossed a threshold. This can arrive earlier allowing
   * implementations to monitor memory more aggressively.
   */
  void handleUsageCrossedNotification();

  /**
   * This indicates any external changes to low memory thresholds that were setup.
   *
   * @param newThresholdPercentage the new threshold percentage.
   */
  void changeLowMemOptions(long newThresholdPercentage, long newAggressiveWidthLowerBound);
}
