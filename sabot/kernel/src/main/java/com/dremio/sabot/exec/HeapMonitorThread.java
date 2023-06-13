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
import java.util.ArrayList;
import java.util.Collection;
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

  // threshold at which clawbacks will start happening
  private final long clawbackThresholdPercentage;

  // threshold at low memory signalling will be done for registered participants. Typically lower than
  // or equal to clawbackThresholdPercentage
  private final long lowMemThresholdPercentage;

  // whether low mem signalling is enabled
  private final boolean lowMemSignallingEnabled;

  // delay after which the heap monitor acts on the threshold exceeded notification
  private final long heapMonitorDelayMillis;

  // map from pool name to collection-threshold-exceeded count.
  private final Map<String, PerPoolInfo> monitoredPools = new HashMap<>();

  // listener for heap notifications.
  private final LowMemListener listener = new LowMemListener();
  private final Collection<HeapLowMemListener> lowMemListeners;

  private boolean shutdown = false;

  public HeapMonitorThread(HeapClawBackStrategy strategy, long clawbackThreshold, long lowMemThreshold,
                           long heapMonitorDelayMillis, Role role,
                           Collection<HeapLowMemListener> lowMemListeners) {
    super();
    setDaemon(true);
    setName("heap-monitoring-thread-"+ role.name().toLowerCase());
    this.strategy = strategy;
    this.clawbackThresholdPercentage = clawbackThreshold;
    this.lowMemSignallingEnabled = lowMemThreshold > 0;
    this.lowMemThresholdPercentage = (lowMemThreshold > 0 && lowMemThreshold < clawbackThreshold) ?
      lowMemThreshold : clawbackThreshold;
    this.heapMonitorDelayMillis = heapMonitorDelayMillis;
    this.lowMemListeners = new ArrayList<>(lowMemListeners);
  }

  private class LowMemListener implements javax.management.NotificationListener {

    @Override
    public void handleNotification(Notification notification, Object handback)  {
      if (notification.getType().equals(MemoryNotificationInfo.MEMORY_COLLECTION_THRESHOLD_EXCEEDED)) {
        // wakeup the main thread.
        synchronized (this) {
          this.notify();
        }
      }
      if (notification.getType().equals(MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED)) {
        logger.info("Heap Memory Usage Threshold notification arrived");
        signalUsageCrossed();
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

        final long lowMemThreshold = (pool.getUsage().getMax() * lowMemThresholdPercentage) / 100;
        final long clawbackThreshold = (pool.getUsage().getMax() * clawbackThresholdPercentage) / 100;
        logger.info("Setting collection threshold for `{}` with max {} to {}", pool.getName(),
          pool.getUsage().getMax(), lowMemThreshold);
        if (lowMemSignallingEnabled) {
          logger.info("Low memory signalling enabled. Usage Threshold set to {} and Clawback Threshold is {}",
            lowMemThreshold, clawbackThreshold);
          pool.setUsageThreshold(lowMemThreshold);
          signalMem(false, pool);
        }

        pool.setCollectionUsageThreshold(lowMemThreshold);
        monitoredPools.put(pool.getName(), new PerPoolInfo(pool.getCollectionUsageThresholdCount(), clawbackThreshold));
      } else {
        logger.info("Skipping monitoring for pool `{}` ", pool.getName());
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

      final PerPoolInfo thisPool = monitoredPools.get(pool.getName());
      if (thisPool.lowMemThresholdCrossed(pool)) {
        logger.info("{} threshold {} for pool `{}` exceeded {} times",
          lowMemSignallingEnabled ? "Low Memory" : "Clawback",
          pool.getCollectionUsageThreshold(), pool.getName(), thisPool.lastExceededCount);
        if (lowMemSignallingEnabled) {
          signalMem(true, pool);
        }
        if (thisPool.clawbackThresholdCrossed(pool)) {

          // Wait for specified time for a short GC to happen, if any
          logger.info("Threshold exceeded notification. HeapMonitor paused for {}ms", heapMonitorDelayMillis);
          Thread.sleep(heapMonitorDelayMillis);

          // Check actual usage has still crossed
          if (thisPool.clawbackThresholdCrossed(pool)) {
            exceeded = true;
            logger.info("Heap usage {} in pool `{}` exceeded clawback threshold {}",
              pool.getUsage().getUsed(),
              pool.getName(), pool.getCollectionUsageThreshold());
            break;
          }
        }
      }
    }
    if (exceeded) {
      strategy.clawBack();
      // block for a while to let the cancel do it's work.
      Thread.sleep(1000);
    }
  }

  @Override
  public void close() {
    shutdown = true;
    interrupt();
  }

  private void signalMem(boolean collectionUsageCrossed, MemoryPoolMXBean pool) {
    if (lowMemSignallingEnabled) {
      for (HeapLowMemListener l : lowMemListeners) {
        l.handleMemNotification(collectionUsageCrossed, pool);
      }
    }
  }

  private void signalUsageCrossed() {
    if (lowMemSignallingEnabled) {
      for (HeapLowMemListener l : lowMemListeners) {
        l.handleUsageCrossedNotification();
      }
    }
  }

  private static final class PerPoolInfo {
    // Tracks number of times threshold was exceeded. Need not be thread safe as it is assumed
    // to be read/updated by a single heap monitor thread.
    private long lastExceededCount;
    private final long clawbackThreshold;

    private PerPoolInfo(long lastExceededCount, long clawbackThreshold) {
      this.lastExceededCount = lastExceededCount;
      this.clawbackThreshold = clawbackThreshold;
    }

    private boolean lowMemThresholdCrossed(MemoryPoolMXBean pool) {
      if (pool.getCollectionUsageThresholdCount() > lastExceededCount) {
        lastExceededCount = pool.getCollectionUsageThresholdCount();
        return true;
      }
      return false;
    }

    private boolean clawbackThresholdCrossed(MemoryPoolMXBean pool) {
      return pool.getUsage().getUsed() >= clawbackThreshold;
    }
  }
}
