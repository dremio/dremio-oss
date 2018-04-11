/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.codahale.metrics.Gauge;
import com.dremio.common.exceptions.UserException;
import com.dremio.common.memory.DremioRootAllocator;
import com.dremio.exec.store.sys.MemoryIterator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocatorFactory;

import com.codahale.metrics.MetricRegistry;
import com.dremio.common.AutoCloseables;
import com.dremio.common.config.SabotConfig;
import com.dremio.common.config.LogicalPlanPersistence;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.config.DremioConfig;
import com.dremio.exec.rpc.CloseableThreadPool;
import com.dremio.metrics.Metrics;

public class BootStrapContext implements AutoCloseable {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(BootStrapContext.class);

  private final SabotConfig config;
  private final MetricRegistry metrics;
  private final BufferAllocator allocator;
  private final ScanResult classpathScan;
  private final CloseableThreadPool executor;
  private final LogicalPlanPersistence lpPersistance;
  private final DremioConfig dremioConfig;
  private final NodeDebugContextProvider nodeDebugContextProvider;

  public BootStrapContext(DremioConfig config, ScanResult classpathScan) {
    this.config = config.getSabotConfig();
    this.classpathScan = classpathScan;
    this.metrics = Metrics.getInstance();
    this.allocator = RootAllocatorFactory.newRoot(config.getSabotConfig());
    this.executor = new CloseableThreadPool("dremio-general-");
    this.lpPersistance = new LogicalPlanPersistence(config.getSabotConfig(), classpathScan);
    this.dremioConfig = config;

    registerMetrics();

    this.nodeDebugContextProvider = (allocator instanceof DremioRootAllocator)
      ? new NodeDebugContextProviderImpl((DremioRootAllocator)allocator)
      : NodeDebugContextProvider.NOOP;
  }

  private void registerMetrics() {
    Metrics.registerGauge(MetricRegistry.name("dremio.memory.direct_current"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return allocator.getAllocatedMemory();
      }
    });

    Metrics.registerGauge(MetricRegistry.name("dremio.memory.jvm_direct_current"), new Gauge<Long>() {
      @Override
      public Long getValue() {
        return MemoryIterator.getDirectBean().getMemoryUsed();
      }
    });
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

  public MetricRegistry getMetrics() {
    return metrics;
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

    executor.close();

    AutoCloseables.closeNoChecked(allocator);
  }

  class NodeDebugContextProviderImpl implements NodeDebugContextProvider {
    private final DremioRootAllocator rootAllocator;

    NodeDebugContextProviderImpl(final DremioRootAllocator rootAllocator) {
      this.rootAllocator = rootAllocator;
    }

    @Override
    public void addMemoryContext(UserException.Builder exceptionBuilder) {
      rootAllocator.addUsageToExceptionContext(exceptionBuilder);
    }
  }
}
