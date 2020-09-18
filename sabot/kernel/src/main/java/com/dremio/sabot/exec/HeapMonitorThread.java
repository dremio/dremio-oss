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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.HashMap;
import java.util.Map;

import javax.management.ListenerNotFoundException;
import javax.management.Notification;
import javax.management.NotificationEmitter;

import com.dremio.service.coordinator.ClusterCoordinator.Role;

/**
 * Monitors the heap space and calls clawBack() on provided HeapClawBackStrategy
 * if heap usage crosses the configured threshold percentage.
 *
 * This is generic enough to be used in both coordinator and executor.
 */
public class HeapMonitorThread extends Thread implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HeapMonitorThread.class);

  // strategy to claw back heap.
  private final HeapClawBackStrategy strategy;

  // threshold at which notifications should be received.
  private final long thresholdPercentage;

  // map from pool name to collection-threshold-exceeded count.
  private Map<String, Long> monitoredPools = new HashMap<>();

  // listener for heap notifications.
  private LowMemListener listener = new LowMemListener();

  private boolean shutdown = false;

  public HeapMonitorThread(HeapClawBackStrategy strategy, long thresholdPercentage, Role role) {
    super();
    setDaemon(true);
    setName("heap-monitoring-thread-"+ role.name().toLowerCase());
    this.strategy = strategy;
    this.thresholdPercentage = thresholdPercentage;
  }

  private class LowMemListener implements javax.management.NotificationListener {

    public void handleNotification(Notification notification, Object handback)  {
      if (notification.getType().equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        // wakeup the main thread.
        synchronized (this) {
          this.notify();
        }
      }
    }
  }

  @Override
  public void run() {
    registerForNotifications();
    while (!shutdown) {
      try {
        synchronized (listener) {
          listener.wait();
        }

        // check heap and take required action.
        checkAndClawBackHeap();
      } catch (final InterruptedException e) {
        logger.debug("Heap monitor thread exiting");
        break;
      } catch (final Exception e) {
        // ignore all other exceptions. The heap monitor thread shouldn't die due to
        // random exceptions.
        logger.debug("Unexpected exception " + e.getMessage());
      }
    }
    deregisterFromNotifications();
  }

  private void registerForNotifications() {
    // Register the listener with MemoryMXBean
    NotificationEmitter emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
    emitter.addNotificationListener(listener, null, null);

    // set collection usage threshold.
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (pool.getType() == MemoryType.HEAP &&
        pool.isUsageThresholdSupported() &&
        pool.isCollectionUsageThresholdSupported()) {

        long threshold = (pool.getUsage().getMax() * thresholdPercentage) / 100;
        logger.info("setting collection threshold for " + pool.getName() +
          " with max " + pool.getUsage().getMax() +
          " to " + threshold);

        pool.setCollectionUsageThreshold(threshold);
        monitoredPools.put(pool.getName(), pool.getCollectionUsageThresholdCount());
      } else {
        logger.info("skip monitoring for pool " + pool.getName());
      }
    }
  }

  private void deregisterFromNotifications() {
    try {
      NotificationEmitter emitter = (NotificationEmitter) ManagementFactory.getMemoryMXBean();
      emitter.removeNotificationListener(this.listener);
    } catch (ListenerNotFoundException e) {
      // ignore error.
      logger.info("missing listener");
    }
  }

  private void checkAndClawBackHeap() throws InterruptedException {
    // verify that at-least one of the pools has exceeded the collection threshold.
    boolean exceeded = false;
    for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
      if (!monitoredPools.containsKey(pool.getName())) {
        continue;
      }

      long thresholdExceededCount = pool.getCollectionUsageThresholdCount();
      if (monitoredPools.get(pool.getName()) < thresholdExceededCount) {
        monitoredPools.put(pool.getName(), thresholdExceededCount);
        exceeded = true;

        logger.info("heap usage " + pool.getUsage().getUsed() +
            " in pool " + pool.getName() +
            " exceeded threshold " + pool.getCollectionUsageThreshold() +
            " threshold_cnt " + pool.getCollectionUsageThresholdCount());
      }
    }
    if (exceeded) {
      strategy.clawBack();
    } else {
      logger.info("spurious wakeup");
    }

    // block for a while to let the cancel do it's work.
    Thread.sleep(1000);
  }

  @Override
  public void close() {
    shutdown = true;
    interrupt();
  }
}
