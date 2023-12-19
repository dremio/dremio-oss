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

import java.util.concurrent.ExecutorService;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.OutOfMemoryException;
import org.apache.arrow.memory.RootAllocatorFactory;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.CloseableExecutorService;
import com.dremio.common.concurrent.CloseableThreadPool;
import com.dremio.common.concurrent.ContextMigratingExecutorService.ContextMigratingCloseableExecutorService;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.common.memory.MemoryDebugInfo;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.exec.store.sys.MemoryIterator;
import com.dremio.service.SingletonRegistry;
import com.dremio.telemetry.api.Telemetry;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;

import io.opentracing.Tracer;

public class BootStrapContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);

  private final SabotConfig config;
  private final BufferAllocator allocator;
  private final ScanResult classpathScan;
  private final CloseableExecutorService executor;
  private final LogicalPlanPersistence lpPersistance;
  private final DremioConfig dremioConfig;
  private final NodeDebugContextProvider nodeDebugContextProvider;

  public BootStrapContext(DremioConfig config, ScanResult classpathScan) {
    this(config, classpathScan, new SingletonRegistry());
  }

  public BootStrapContext(DremioConfig config, ScanResult classpathScan, SingletonRegistry registry) {
    registry.bind(Tracer.class, TracerFacade.INSTANCE);
    registry.bind(GrpcTracerFacade.class, new GrpcTracerFacade(TracerFacade.INSTANCE));
    Telemetry.startTelemetry();

    this.config = config.getSabotConfig();
    this.classpathScan = classpathScan;
    this.allocator = RootAllocatorFactory.newRoot(config);
    this.executor = new ContextMigratingCloseableExecutorService<>(new CloseableThreadPool("dremio-general-"));
    this.lpPersistance = new LogicalPlanPersistence(config.getSabotConfig(), classpathScan);
    this.dremioConfig = config;

    registerMetrics();

    this.nodeDebugContextProvider = (allocator instanceof DremioRootAllocator)
      ? new NodeDebugContextProviderImpl((DremioRootAllocator) allocator)
      : NodeDebugContextProvider.NOOP;
  }

  private void registerMetrics() {
    Metrics.newGauge("dremio.memory.direct_current", allocator::getAllocatedMemory);

    if (allocator instanceof DremioRootAllocator) {
      Metrics.newGauge("dremio.memory.remaining_heap_allocations", ((DremioRootAllocator) allocator)::getAvailableBuffers);
    }

    Metrics.newGauge("dremio.memory.jvm_direct_current", MemoryIterator.getDirectBean()::getMemoryUsed);
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

  public NodeDebugContextProvider getNodeDebugContextProvider() {
    return nodeDebugContextProvider;
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
      logger.warn("Failure while closing dremio-general thread pool", e);
    }

    AutoCloseables.closeNoChecked(allocator);
  }

  class NodeDebugContextProviderImpl implements NodeDebugContextProvider {
    private final DremioRootAllocator rootAllocator;

    NodeDebugContextProviderImpl(final DremioRootAllocator rootAllocator) {
      this.rootAllocator = rootAllocator;
    }

    @Override
    public void addMemoryContext(UserException.Builder exceptionBuilder) {
      String detail = MemoryDebugInfo.getSummaryFromRoot(rootAllocator);
      exceptionBuilder.addContext(detail);
    }

    @Override
    public void addMemoryContext(UserException.Builder exceptionBuilder, OutOfMemoryException e) {
      String detail = MemoryDebugInfo.getDetailsOnAllocationFailure(e, rootAllocator);
      exceptionBuilder.addContext(detail);
    }
  }
}
