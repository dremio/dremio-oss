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
package com.dremio.metrics;

import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.dremio.common.config.SabotConfig;
import com.google.common.collect.ImmutableMap;

/**
 * Dremio main metrics class
 */
public final class Metrics {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metrics.class);

  public static final String METRICS_JMX_OUTPUT_ENABLED = "dremio.metrics.jmx.enabled";
  public static final String METRICS_LOG_OUTPUT_ENABLED = "dremio.metrics.log.enabled";
  public static final String METRICS_LOG_OUTPUT_INTERVAL = "dremio.metrics.log.interval";

  private static final SabotConfig config = SabotConfig.create();

  private Metrics() {
  }

  private static class RegistryHolder {
    public static final MetricRegistry REGISTRY;
    private static final JmxReporter JMX_REPORTER;
    private static final Slf4jReporter LOG_REPORTER;

    static {
      REGISTRY = new MetricRegistry();
      registerSysStats();
      JMX_REPORTER = getJmxReporter();
      LOG_REPORTER = getLogReporter();
    }

    private static void registerSysStats(){
      REGISTRY.registerAll(scoped("gc", new GarbageCollectorMetricSet()));
      REGISTRY.registerAll(scoped("buffer-pool", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer())));
      REGISTRY.registerAll(scoped("memory", new MemoryUsageGaugeSet()));
      REGISTRY.registerAll(scoped("threads", new ThreadStatesGaugeSet()));
    }

    private static JmxReporter getJmxReporter() {
      if (config.getBoolean(METRICS_JMX_OUTPUT_ENABLED)) {
        JmxReporter reporter = JmxReporter.forRegistry(getInstance()).build();
        reporter.start();

        return reporter;
      } else {
        return null;
      }
    }

    private static Slf4jReporter getLogReporter() {
      if (config.getBoolean(METRICS_LOG_OUTPUT_ENABLED)) {
        Slf4jReporter reporter = Slf4jReporter.forRegistry(getInstance()).outputTo(logger)
            .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(config.getInt(METRICS_LOG_OUTPUT_INTERVAL), TimeUnit.SECONDS);

        return reporter;
      } else {
        return null;
      }
    }
  }

  public static synchronized void registerGauge(String name, Gauge<?> metric) {
    boolean removed = RegistryHolder.REGISTRY.remove(name);
    if (removed) {
      logger.warn("Removing old metric since name matched newly registered metric. Metric name: {}", name);
    }
    RegistryHolder.REGISTRY.register(name, metric);
  }

  public static MetricRegistry getInstance() {
    return RegistryHolder.REGISTRY;
  }

  private static MetricSet scoped(final String name, final MetricSet metricSet) {
    return new MetricSet() {
      @Override
      public Map<String, Metric> getMetrics() {
        ImmutableMap.Builder<String, Metric> scopedMetrics = ImmutableMap.builder();
        for(Map.Entry<String, Metric> entry: metricSet.getMetrics().entrySet()) {
          scopedMetrics.put(MetricRegistry.name(name, entry.getKey()), entry.getValue());
        }
        return scopedMetrics.build();
      }
    };
  }

  public static void resetMetrics(){
    RegistryHolder.REGISTRY.removeMatching(new MetricFilter(){
      @Override
      public boolean matches(String name, Metric metric) {
        return true;
      }});
    RegistryHolder.registerSysStats();
  }

}
