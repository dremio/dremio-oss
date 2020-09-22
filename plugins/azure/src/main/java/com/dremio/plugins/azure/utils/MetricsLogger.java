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

package com.dremio.plugins.azure.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;

/**
 * Captures the latency breakups and logs them in the end.
 */
public final class MetricsLogger {
  private final Map<String, Stopwatch> watchers = new HashMap<>();
  private final Map<String, Integer> counters = new HashMap<>();
  private static final Logger logger = LoggerFactory.getLogger(MetricsLogger.class);

  public void incrementCounter(String key) {
    if (logger.isDebugEnabled()) {
      int currentVal = counters.getOrDefault(key, 0);
      counters.put(key, ++currentVal);
    }
  }

  public void startTimer(String key) {
    if (logger.isDebugEnabled()) {
      watchers.put(key, Stopwatch.createStarted());
    }
  }

  public void endTimer(String key) {
    if (logger.isDebugEnabled()) {
      watchers.get(key).stop();
    }
  }

  public void logAllMetrics() {
    if (logger.isDebugEnabled()) {
      StringBuffer buffer = new StringBuffer();
      buffer.append(Thread.currentThread().getId());
      buffer.append(" [METRICS] ");
      for (Map.Entry<String, Stopwatch> watcherEntry : watchers.entrySet()) {
        buffer.append(watcherEntry.getKey() + "=" + watcherEntry.getValue().elapsed(TimeUnit.MILLISECONDS) + ", ");
      }
      if (!counters.isEmpty()) {
        buffer.append("[COUNTERS] ");
        for (Map.Entry<String, Integer> counterEntry : counters.entrySet()) {
          buffer.append(counterEntry.getKey() + "=" + counterEntry.getValue() + ", ");
        }
      }
      logger.debug(buffer.toString());
    }
  }
}
