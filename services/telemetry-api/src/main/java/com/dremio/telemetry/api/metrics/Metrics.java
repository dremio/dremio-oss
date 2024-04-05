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
package com.dremio.telemetry.api.metrics;

import static io.micrometer.prometheus.PrometheusConfig.DEFAULT;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer.Context;
import com.codahale.metrics.jetty9.InstrumentedHttpChannelListener;
import com.codahale.metrics.jetty9.InstrumentedQueuedThreadPool;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.github.rollingmetrics.counter.ResetOnSnapshotCounter;
import com.github.rollingmetrics.counter.ResetPeriodicallyCounter;
import com.github.rollingmetrics.counter.SmoothlyDecayingRollingCounter;
import com.github.rollingmetrics.counter.WindowCounter;
import com.github.rollingmetrics.histogram.HdrBuilder;
import com.github.rollingmetrics.top.Top;
import com.github.rollingmetrics.top.TopBuilder;
import com.github.rollingmetrics.top.TopMetricSet;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jvm.ClassLoaderMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmHeapPressureMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.StandardExports;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import javax.servlet.Servlet;

/** Dremio main metrics class */
public final class Metrics {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metrics.class);
  private static final int DECAY_CHUNKS = 10;

  /** Container for Metrics Registry holder and the reporter manager. */
  @VisibleForTesting
  public static class RegistryHolder {
    private static final String GC = "gc";
    private static final String BUFFER_POOL = "buffer-pool";
    private static final String MEMORY = "memory";
    private static final String THREADS = "threads";
    // package private to enable testing
    static final Tags LEGACY_TAGS = Tags.of(Tag.of("metric_type", "legacy"));

    private static MetricRegistry codahaleMetricRegistry;
    private static ReporterManager reporters;
    private static PrometheusMeterRegistry prometheusMeterRegistry;

    static {
      initRegistry();
    }

    @VisibleForTesting
    public static void initRegistry() {
      prometheusMeterRegistry =
          io.micrometer.core.instrument.Metrics.globalRegistry.getRegistries().stream()
              .filter(registry -> registry instanceof PrometheusMeterRegistry)
              .findFirst()
              .map(registry -> (PrometheusMeterRegistry) registry)
              .orElseGet(
                  () -> {
                    // OSS use case -- need to create a new PrometheusMeterRegistry and add it to
                    // the global registry
                    PrometheusMeterRegistry newPrometheusRegistry =
                        new PrometheusMeterRegistry(DEFAULT);
                    newPrometheusRegistry.getPrometheusRegistry().register(new StandardExports());

                    io.micrometer.core.instrument.Metrics.globalRegistry.add(newPrometheusRegistry);

                    Stream.of(
                            new ClassLoaderMetrics(),
                            new JvmHeapPressureMetrics(),
                            new JvmMemoryMetrics(),
                            new JvmThreadMetrics(),
                            new JvmGcMetrics(),
                            new UptimeMetrics(),
                            new ProcessorMetrics(),
                            new FileDescriptorMetrics())
                        .forEach(
                            binder ->
                                binder.bindTo(
                                    io.micrometer.core.instrument.Metrics.globalRegistry));

                    return newPrometheusRegistry;
                  });

      // need codahale metric registry until we convert all clients to Micrometer
      codahaleMetricRegistry = new MetricRegistry();
      registerSysStats();

      CollectorRegistry collectorRegistry = prometheusMeterRegistry.getPrometheusRegistry();
      collectorRegistry.register(
          new DropwizardExports(
              RegistryHolder.codahaleMetricRegistry,
              new DefaultSampleBuilder() {
                @Override
                public Collector.MetricFamilySamples.Sample createSample(
                    final String dropwizardName,
                    final String nameSuffix,
                    final List<String> additionalLabelNames,
                    final List<String> additionalLabelValues,
                    final double value) {
                  if (Stream.of(GC, BUFFER_POOL, MEMORY, THREADS)
                      .noneMatch(prefix -> dropwizardName.startsWith(prefix))) {
                    List<String> newAdditionalLabelNames = new ArrayList<>(additionalLabelNames);
                    List<String> newAdditionalLabelValues = new ArrayList<>(additionalLabelValues);

                    LEGACY_TAGS.forEach(
                        tag -> {
                          newAdditionalLabelNames.add(tag.getKey());
                          newAdditionalLabelValues.add(tag.getValue());
                        });

                    return super.createSample(
                        dropwizardName,
                        nameSuffix,
                        newAdditionalLabelNames,
                        newAdditionalLabelValues,
                        value);
                  } else {
                    return super.createSample(
                        dropwizardName,
                        nameSuffix,
                        additionalLabelNames,
                        additionalLabelValues,
                        value);
                  }
                }
              }));

      reporters = new ReporterManager(codahaleMetricRegistry);
    }

    private static void registerSysStats() {
      codahaleMetricRegistry.registerAll(scoped(GC, new GarbageCollectorMetricSet()));
      codahaleMetricRegistry.registerAll(
          scoped(BUFFER_POOL, new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer())));
      codahaleMetricRegistry.registerAll(scoped(MEMORY, new MemoryUsageGaugeSet()));
      codahaleMetricRegistry.registerAll(scoped(THREADS, new ThreadStatesGaugeSet()));
    }
  }

  /**
   * Helper class for constructing MetricServlets. This is a separate factory class to avoid
   * introducing a dependency on Servlet to the enclosing Metrics class.
   */
  public static class MetricServletFactory {
    /**
     * Creates a custom Prometheus Collector registry, registers the dropwizard metrics with this
     * registry, and attaches it to a MetricsServlet, which is returned to the caller.
     */
    public static Servlet createMetricsServlet() {
      return new MetricsServlet(RegistryHolder.prometheusMeterRegistry.getPrometheusRegistry());
    }
  }

  private static synchronized void remove(String name) {
    if (RegistryHolder.codahaleMetricRegistry.remove(name)) {
      logger.warn(
          "Removing old metric since name matched newly registered metric. Metric name: {}", name);
    }
  }

  private static MetricSet scoped(final String name, final MetricSet metricSet) {
    return new MetricSet() {
      @Override
      public Map<String, Metric> getMetrics() {
        ImmutableMap.Builder<String, Metric> scopedMetrics = ImmutableMap.builder();
        for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
          scopedMetrics.put(MetricRegistry.name(name, entry.getKey()), entry.getValue());
        }
        return scopedMetrics.build();
      }
    };
  }

  public static void onChange(Collection<MetricsConfigurator> updated) {
    RegistryHolder.reporters.onChange(updated);
  }

  public static void resetMetrics() {
    io.micrometer.core.instrument.Metrics.globalRegistry.forEachMeter(
        io.micrometer.core.instrument.Metrics.globalRegistry::remove);
    RegistryHolder.codahaleMetricRegistry.removeMatching((a, b) -> true);
    RegistryHolder.registerSysStats();
  }

  public static synchronized void unregister(String metric) {
    if (!RegistryHolder.codahaleMetricRegistry.remove(metric)) {
      logger.warn("Could not find metric to unregister. Metric name: {}", metric);
    }
  }

  @VisibleForTesting
  public static Collection<MetricsConfigurator> getConfigurators() {
    return RegistryHolder.reporters.getParentConfigurators();
  }

  public static synchronized void newGauge(String name, LongSupplier supplier) {
    remove(name);
    RegistryHolder.codahaleMetricRegistry.register(name, (Gauge<Long>) supplier::getAsLong);
  }

  public static synchronized Counter newCounter(String name, ResetType reset) {
    remove(name);
    switch (reset) {
      case NEVER:
        com.codahale.metrics.Counter inner = RegistryHolder.codahaleMetricRegistry.counter(name);
        return new Counter() {
          @Override
          public void increment(long value, String... tags) {
            inner.inc(value);
          }

          @Override
          public void decrement(long value, String... tags) {
            inner.dec(value);
          }

          @Override
          public String toString() {
            return String.valueOf(inner.getCount());
          }
        };

      case ON_SNAPSHOT:
        return registerWindowCounter(name, new ResetOnSnapshotCounter());
      case PERIODIC_1S:
      case PERIODIC_15M:
      case PERIODIC_1D:
      case PERIODIC_7D:
        return registerWindowCounter(name, new ResetPeriodicallyCounter(reset.getDuration()));
      case PERIODIC_DECAY:
        return registerWindowCounter(
            name, new SmoothlyDecayingRollingCounter(reset.getDuration(), DECAY_CHUNKS));
      default:
        throw new UnsupportedOperationException("Unknown type: " + reset);
    }
  }

  private static Counter registerWindowCounter(String name, WindowCounter counter) {
    RegistryHolder.codahaleMetricRegistry.register(name, (Gauge<Long>) counter::getSum);
    return new Counter() {

      @Override
      public void increment(long value, String... tags) {
        counter.add(value);
      }

      @Override
      public void decrement(long value, String... tags) {
        counter.add(-value);
      }

      @Override
      public String toString() {
        return String.valueOf(counter.getSum());
      }
    };
  }

  /** The type of reset behavior a Counter/Histogram/Etc should have. */
  public static enum ResetType {
    /** Never reset value. */
    NEVER,

    /** Reset each time value is observed. */
    ON_SNAPSHOT,

    /** Reset values every 15 minutes. */
    PERIODIC_15M(Duration.ofMinutes(15)),

    /** Reset every day */
    PERIODIC_1D(Duration.ofDays(1)),

    /** Reset every week */
    PERIODIC_7D(Duration.ofDays(7)),

    /** Do multiple trackers. To be implemented. */
    MULTI(Duration.ofDays(7)),

    /** Reset periodically with decay. */
    PERIODIC_DECAY(Duration.ofDays(7)),

    /** For testing only */
    PERIODIC_1S(Duration.ofSeconds(1));

    private Duration duration;

    ResetType() {
      this.duration = null;
    }

    ResetType(Duration duration) {
      this.duration = duration;
    }

    public Duration getDuration() {
      return duration;
    }
  }

  /**
   * Create a new reporter that reports the top slow operations over time.
   *
   * @param name The name of the metric
   * @param count The number of items to keep (eg 10 for top 10)
   * @param latencyThreshold The latency below which observations should be ignored.
   * @param reset The desired reset behavior.
   * @return The reporter to use.
   */
  public static synchronized TopMonitor newTopReporter(
      String name, int count, Duration latencyThreshold, ResetType reset) {
    TopBuilder builder = Top.builder(count).withLatencyThreshold(latencyThreshold);
    switch (reset) {
      case NEVER:
        builder.neverResetPositions();
        break;
      case ON_SNAPSHOT:
        builder.resetAllPositionsOnSnapshot();
        break;
      case PERIODIC_15M:
      case PERIODIC_1D:
      case PERIODIC_7D:
        builder.resetAllPositionsPeriodically(reset.getDuration());
        break;
      case PERIODIC_DECAY:
        builder.resetPositionsPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    Top top = builder.build();
    MetricSet metricSet = new TopMetricSet(name, top, TimeUnit.MILLISECONDS, 2);
    RegistryHolder.codahaleMetricRegistry.registerAll(metricSet);
    return (latency, desc, tags) ->
        top.update(System.currentTimeMillis(), latency, TimeUnit.MILLISECONDS, () -> desc.get());
  }

  /** Tracks rate of occurrence. For example: requests per second. */
  public static synchronized Meter newMeter(String name) {
    remove(name);
    com.codahale.metrics.Meter meter = RegistryHolder.codahaleMetricRegistry.meter(name);
    return (tags) -> meter.mark();
  }

  /** Create a histogram of values. */
  public static synchronized Histogram newHistogram(String name, ResetType reset) {
    remove(name);
    HdrBuilder builder = new HdrBuilder();

    switch (reset) {
      case NEVER:
        com.codahale.metrics.Histogram histogram =
            RegistryHolder.codahaleMetricRegistry.histogram(name);
        return (v, tags) -> histogram.update(v);
      case ON_SNAPSHOT:
        builder.resetReservoirOnSnapshot();
        break;
      case PERIODIC_15M:
      case PERIODIC_1D:
      case PERIODIC_7D:
        builder.resetReservoirPeriodically(reset.getDuration());
        break;
      case PERIODIC_DECAY:
        builder.resetReservoirPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    com.codahale.metrics.Histogram hist =
        builder.buildAndRegisterHistogram(RegistryHolder.codahaleMetricRegistry, name);
    return (v, tags) -> hist.update(v);
  }

  /** Create a timer to observe length of operations. */
  public static synchronized Timer newTimer(String name, ResetType reset) {
    remove(name);

    HdrBuilder builder = new HdrBuilder();
    switch (reset) {
      case NEVER:
        return asPublicTimer(RegistryHolder.codahaleMetricRegistry.timer(name));
      case ON_SNAPSHOT:
        builder.resetReservoirOnSnapshot();
        break;
      case PERIODIC_15M:
      case PERIODIC_1D:
      case PERIODIC_7D:
        builder.resetReservoirPeriodically(reset.getDuration());
        break;
      case PERIODIC_DECAY:
        builder.resetReservoirPeriodicallyByChunks(reset.getDuration(), DECAY_CHUNKS);
        break;
      default:
        throw new UnsupportedOperationException("Unknown type: " + reset);
    }

    return asPublicTimer(
        builder.buildAndRegisterTimer(RegistryHolder.codahaleMetricRegistry, name));
  }

  private static Timer asPublicTimer(com.codahale.metrics.Timer timer) {
    return new Timer() {
      @Override
      public TimerContext start(String... tags) {
        Context ctxt = timer.time();
        return () -> ctxt.close();
      }

      @Override
      public void update(long duration, TimeUnit timeUnit) {
        timer.update(duration, timeUnit);
      }
    };
  }

  /**
   * Join metrics references by period.
   *
   * @param fields
   * @return A period-based concatenated string
   */
  public static String join(String... fields) {
    return Joiner.on('.').join(fields);
  }

  /**
   * Tracks various metrics for Jetty server. This function provides an instance of Jetty {@link
   * org.eclipse.jetty.server.HttpChannel.Listener} injected with appropriate Metric Registry.
   *
   * @param prefix The prefix for metrics captured by this Channel Listener.
   * @return A Channel Listener with Metric Registry injected.
   */
  public static InstrumentedHttpChannelListener newInstrumentedListener(String prefix) {
    return new InstrumentedHttpChannelListener(RegistryHolder.codahaleMetricRegistry, prefix);
  }

  /**
   * Helper function to provide a QueuedThreadPool instance for Jetty Server injected with
   * appropriate Metric Registry. <br>
   * <b>NOTE:</b>The threadpool is instantiated with default setting for max and min threads,
   * timeout etc. . If any tuning is required, needs to be done after acquiring the instance.
   *
   * @param prefix The prefix for metrics captured by this QueuedThreadPool.
   * @return a QueuedThreadPool instance injected with Metric Registry.
   */
  public static InstrumentedQueuedThreadPool newInstrumentedThreadPool(String prefix) {
    InstrumentedQueuedThreadPool instrumentedQTP =
        new InstrumentedQueuedThreadPool(RegistryHolder.codahaleMetricRegistry);
    instrumentedQTP.setPrefix(prefix);
    return instrumentedQTP;
  }
}
