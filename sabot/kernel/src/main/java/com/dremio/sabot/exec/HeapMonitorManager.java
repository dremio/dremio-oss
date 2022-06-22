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

import static com.dremio.exec.ExecConstants.COORDINATOR_ENABLE_HEAP_MONITORING;
import static com.dremio.exec.ExecConstants.COORDINATOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE;
import static com.dremio.exec.ExecConstants.COORDINATOR_HEAP_MONITOR_DELAY_MILLIS;
import static com.dremio.exec.ExecConstants.EXECUTOR_ENABLE_HEAP_MONITORING;
import static com.dremio.exec.ExecConstants.EXECUTOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE;
import static com.dremio.exec.ExecConstants.EXECUTOR_HEAP_MONITOR_DELAY_MILLIS;

import javax.inject.Provider;

import com.dremio.options.OptionChangeListener;
import com.dremio.options.OptionManager;
import com.dremio.options.TypeValidators.AdminBooleanValidator;
import com.dremio.options.TypeValidators.RangeLongValidator;
import com.dremio.service.Service;
import com.dremio.service.coordinator.ClusterCoordinator.Role;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

/**
 * Manages heap monitor thread in coordinator and executor.
 */
public class HeapMonitorManager implements Service {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(HeapMonitorManager.class);
  private final Provider<OptionManager> optionManagerProvider;
  private final HeapClawBackStrategy heapClawBackStrategy;
  private HeapMonitorThread heapMonitorThread;
  private AdminBooleanValidator enableHeapMonitoringOption;
  private RangeLongValidator clawBackThresholdOption;
  private RangeLongValidator heapMonitorDelayOption;
  private Role role;

  public HeapMonitorManager(Provider<OptionManager> optionManagerProvider,
                            HeapClawBackStrategy heapClawBackStrategy,
                            Role role) {
    Preconditions.checkNotNull(optionManagerProvider);
    Preconditions.checkNotNull(heapClawBackStrategy);
    this.optionManagerProvider = optionManagerProvider;
    this.heapClawBackStrategy = heapClawBackStrategy;
    this.role = role;

    switch(role) {
      case COORDINATOR:
        this.enableHeapMonitoringOption = COORDINATOR_ENABLE_HEAP_MONITORING;
        this.clawBackThresholdOption = COORDINATOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE;
        this.heapMonitorDelayOption = COORDINATOR_HEAP_MONITOR_DELAY_MILLIS;
        break;
      case EXECUTOR:
        this.enableHeapMonitoringOption = EXECUTOR_ENABLE_HEAP_MONITORING;
        this.clawBackThresholdOption = EXECUTOR_HEAP_MONITORING_CLAWBACK_THRESH_PERCENTAGE;
        this.heapMonitorDelayOption = EXECUTOR_HEAP_MONITOR_DELAY_MILLIS;
        break;
      default:
        throw new UnsupportedOperationException("Heap monitor manager cannot be configured for provided role:" + role.name());
    }
  }

  public void start() {
    OptionManager optionManager = optionManagerProvider.get();
    startHeapMonitorThread(optionManager.getOption(enableHeapMonitoringOption),
      optionManager.getOption(clawBackThresholdOption),
      optionManager.getOption(heapMonitorDelayOption));
    optionManager.addOptionChangeListener(new HeapOptionChangeListener(optionManager));
  }

  // Start heap monitor thread, if heap monitoring is enabled
  private void startHeapMonitorThread(boolean enableHeapMonitoring, long thresholdPercentage, long heapMonitorDelayMillis) {
    if (enableHeapMonitoring) {
      logger.info("Starting heap monitor thread in " + role.name().toLowerCase());
      heapMonitorThread = new HeapMonitorThread(heapClawBackStrategy, thresholdPercentage, heapMonitorDelayMillis, role);
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
    private long heapMonitorDelayMillis;
    private OptionManager optionManager;

    public HeapOptionChangeListener(OptionManager optionManager) {
      this.optionManager = optionManager;
      this.enableHeapMonitoring = optionManager.getOption(enableHeapMonitoringOption);
      this.thresholdPercentage = optionManager.getOption(clawBackThresholdOption);
      this.heapMonitorDelayMillis = optionManager.getOption(heapMonitorDelayOption);
    }

    @Override
    public synchronized void onChange() {
      boolean newEnableHeapMonitoring = optionManager.getOption(enableHeapMonitoringOption);
      long newThresholdPercentage = optionManager.getOption(clawBackThresholdOption);
      long newHeapMonitorDelayMillis = optionManager.getOption(heapMonitorDelayOption);
      if (newEnableHeapMonitoring != enableHeapMonitoring ||
        newThresholdPercentage != thresholdPercentage || newHeapMonitorDelayMillis != heapMonitorDelayMillis) {
        logger.info("Heap monitor options changed.");
        stopHeapMonitorThread();
        enableHeapMonitoring = newEnableHeapMonitoring;
        thresholdPercentage = newThresholdPercentage;
        heapMonitorDelayMillis = newHeapMonitorDelayMillis;
        startHeapMonitorThread(enableHeapMonitoring, thresholdPercentage, heapMonitorDelayMillis);
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
