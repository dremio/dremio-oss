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

import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.google.common.annotations.VisibleForTesting;

public class HeapMonitorManager implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HeapMonitorManager.class);
  private final OptionManager optionManager;
  private final FragmentExecutors fragmentExecutors;
  private final QueriesClerk queriesClerk;
  private HeapMonitorThread heapMonitorThread;

  public HeapMonitorManager(OptionManager optionManager, FragmentExecutors fragmentExecutors, QueriesClerk queriesClerk) {
    this.optionManager = optionManager;
    this.fragmentExecutors = fragmentExecutors;
    this.queriesClerk = queriesClerk;
    startHeapMonitorThread(optionManager.getOption(ExecConstants.ENABLE_HEAP_MONITORING),
                           optionManager.getOption(ExecConstants.HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE));
    optionManager.addOptionChangeListener(new HeapOptionChangeListener(optionManager));
  }

  // Start heap monitor thread, if heap monitoring is enabled
  private void startHeapMonitorThread(boolean enableHeapMonitoring, long thresholdPercentage) {
    if (enableHeapMonitoring) {
      logger.info("Starting heap monitor thread...");
      HeapClawBackStrategy strategy = new FailGreediestQueriesStrategy(fragmentExecutors, queriesClerk);
      heapMonitorThread = new HeapMonitorThread(strategy, thresholdPercentage);
      heapMonitorThread.start();
    }
  }

  @VisibleForTesting
  public boolean isHeapMonitorThreadRunning() {
    return heapMonitorThread != null;
  }

  private class HeapOptionChangeListener implements OptionChangeListener {
    private boolean enableHeapMonitoring;
    private long thresholdPercentage;
    private OptionManager optionManager;

    public HeapOptionChangeListener(OptionManager optionManager) {
      this.optionManager = optionManager;
      this.enableHeapMonitoring = optionManager.getOption(ExecConstants.ENABLE_HEAP_MONITORING);
      this.thresholdPercentage = optionManager.getOption(ExecConstants.HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE);
    }

    @Override
    public synchronized void onChange() {
      boolean newEnableHeapMonitoring = optionManager.getOption(ExecConstants.ENABLE_HEAP_MONITORING);
      long newThresholdPercentage = optionManager.getOption(ExecConstants.HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE);
      if (newEnableHeapMonitoring != enableHeapMonitoring ||
        newThresholdPercentage != thresholdPercentage) {
        logger.info("Heap monitor options changed.");
        stopHeapMonitorThread();
        enableHeapMonitoring = newEnableHeapMonitoring;
        thresholdPercentage = newThresholdPercentage;
        startHeapMonitorThread(enableHeapMonitoring, thresholdPercentage);
      }
    }
  }

  private void stopHeapMonitorThread() {
    if (heapMonitorThread != null) {
      logger.info("Stopping heap monitor thread");
      heapMonitorThread.close();
      heapMonitorThread = null;
    }
  }

  @Override
  public void close() {
    stopHeapMonitorThread();
  }
}
