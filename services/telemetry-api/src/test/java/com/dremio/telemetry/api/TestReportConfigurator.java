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
package com.dremio.telemetry.api;

import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.dremio.telemetry.api.config.ReporterConfigurator;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * A configurator for test purposes.
 */
@JsonTypeName("test")
public class TestReportConfigurator extends ReporterConfigurator {

  private TestReporter reporter;

  @Override
  public void configureAndStart(String name, MetricRegistry registry, MetricFilter filter) {
    reporter = new TestReporter(registry, name, filter);
    reporter.start(10, TimeUnit.MILLISECONDS);
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }

  public long getCount() {
    return reporter.getCount();
  }

  @Override
  public void close() {
    reporter.close();
  }

  private class TestReporter extends ScheduledReporter {

    private AtomicLong count = new AtomicLong();

    public TestReporter(
        MetricRegistry registry,
        String name,
        MetricFilter filter
        ) {
      super(registry, name, filter, TimeUnit.SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void report(
        SortedMap<String, Gauge> gauges,
        SortedMap<String, Counter> counters,
        SortedMap<String, Histogram> histograms,
        SortedMap<String, Meter> meters,
        SortedMap<String, Timer> timers) {
      count.getAndIncrement();
    }

    public long getCount() {
      return count.get();
    }
  }
}
