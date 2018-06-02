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
package com.dremio.datastore;


import java.util.Map;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.dremio.metrics.Metrics;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * Helper methods and class to facilitate collecting kvstore related metrics
 */
public class MetricUtils {
  public static final boolean COLLECT_METRICS = System.getProperty("dremio.kvstore.metrics", null) != null;

  public static final CloseableTimer NO_OP = new CloseableTimer() {};

  public static void removeAllMetricsThatStartWith(final String prefix) {
    Metrics.getInstance().removeMatching((String name, Metric metric) -> name.startsWith(prefix));
  }

  /**
   * {@link MetricSet} builder
   */
  public static class MetricSetBuilder {
    private final ImmutableMap.Builder<String, Metric> map = ImmutableMap.builder();
    private final String metricPrefix;

    public MetricSetBuilder(String metricPrefix) {
      this.metricPrefix = Preconditions.checkNotNull(metricPrefix, "metric prefix required");
    }

    public MetricSetBuilder gauge(final String name, final Gauge<Long> gauge) {
      map.put(MetricRegistry.name(metricPrefix, name), gauge);
      return this;
    }

    public MetricSet build() {
      final Map<String, Metric> metrics = map.build();
      return () -> metrics;
    }
  }

  /**
   * Closeable timer
   */
  public abstract static class CloseableTimer implements AutoCloseable {
    @Override
    public void close() {}
  }

  /**
   * Metric timer that tracks the time between the constructor and close method. Can be used in a try with resources
   * to report the duration of the try block.
   */
  public static class MetricTimer extends CloseableTimer {

    private final Timer.Context context;

    MetricTimer(Timer timer) {
      Preconditions.checkNotNull(timer, "timer required");
      context = timer.time();
    }

    @Override
    public void close() {
      context.close();
    }
  }

}
