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
package com.dremio.exec.server;

import static com.dremio.telemetry.api.metrics.MeterProviders.newGauge;

import com.dremio.common.AutoCloseables;
import com.dremio.common.VM;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService.ContextMigratingCloseableExecutorService;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.exec.store.sys.MemoryIterator;
import com.dremio.telemetry.api.Telemetry;
import com.dremio.telemetry.api.metrics.Metrics;
import io.netty.util.internal.PlatformDependent;
import java.util.concurrent.ExecutorService;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;

public class BootStrapContext implements AutoCloseable {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);

  private final SabotConfig config;
  private final BufferAllocator allocator;
  private final ScanResult classpathScan;
  private final CloseableExecutorService executor;
  private final LogicalPlanPersistence lpPersistance;
  private final DremioConfig dremioConfig;

  public BootStrapContext(DremioConfig config, ScanResult classpathScan) {
    Telemetry.startTelemetry();

    this.config = config.getSabotConfig();
    this.classpathScan = classpathScan;
    this.allocator = RootAllocatorFactory.newRoot(config);
    this.executor =
        new ContextMigratingCloseableExecutorService<>(new CloseableThreadPool("dremio-general-"));
    this.lpPersistance = new LogicalPlanPersistence(classpathScan);
    this.dremioConfig = config;

    registerMetrics();
  }

  private void registerMetrics() {
    newGauge("dremio.memory.apache_arrow_direct_current", allocator::getAllocatedMemory);
    newGauge("dremio.memory.apache_arrow_direct_memory_peak", allocator::getPeakMemoryAllocation);
    newGauge("dremio.memory.apache_arrow_direct_memory_max", allocator::getLimit);
    newGauge("dremio.memory.netty_direct_memory_current", PlatformDependent::usedDirectMemory);
    newGauge("dremio.memory.netty_direct_memory_max", PlatformDependent::maxDirectMemory);

    if (allocator instanceof DremioRootAllocator) {
      newGauge(
          "dremio.memory.buffers_remaining_count",
          ((DremioRootAllocator) allocator)::getAvailableBuffers);
      newGauge("dremio.memory.buffers_max", ((DremioRootAllocator) allocator)::getMaxBufferCount);
    }

    newGauge("dremio.memory.jvm_direct_memory_max", VM::getMaxDirectMemory);
    newGauge("dremio.memory.jvm_direct_current", MemoryIterator.getDirectBean()::getMemoryUsed);

    newGauge(
        "dremio.memory.jvm_metspace_current",
        MemoryIterator.getMetaspaceBean().getUsage()::getUsed);
    newGauge(
        "dremio.memory.jvm_metspace_committed",
        MemoryIterator.getMetaspaceBean().getUsage()::getCommitted);
    newGauge(
        "dremio.memory.jvm_metspace_max", MemoryIterator.getMetaspaceBean().getUsage()::getMax);
  }

  public DremioConfig getDremioConfig() {
    return dremioConfig;
  }

  public ExecutorService getExecutor() {
    return executor;
  }

  public SabotConfig getConfig() {
    return config;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }

  public ScanResult getClasspathScan() {
    return classpathScan;
  }

  public LogicalPlanPersistence getLpPersistance() {
    return lpPersistance;
  }

  @Override
  public void close() {
    try {
      Metrics.resetMetrics();
    } catch (Error | Exception e) {
      logger.warn("failure resetting metrics.", e);
    }

    try {
      executor.close();
    } catch (Exception e) {
      if (VM.areAssertsEnabled()) {
        throw new RuntimeException(e);
      }
      logger.warn("Failure while closing dremio-general thread pool", e);
    }

    AutoCloseables.closeNoChecked(allocator);
  }
}
