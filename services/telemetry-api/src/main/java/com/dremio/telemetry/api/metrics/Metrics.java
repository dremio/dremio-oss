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

import static io.micrometer.core.instrument.Metrics.globalRegistry;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Meter;
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
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.HistogramFlavor;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.StandardExports;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.servlet.Servlet;
import org.jetbrains.annotations.NotNull;

/** Dremio main metrics class */
public final class Metrics {
  public static final String DREMIO_MICROMETER_HISTOGRAM_FLAVOR_ENV_VAR =
      "DREMIO_MICROMETER_HISTOGRAM_FLAVOR";

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Metrics.class);

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

    public static PrometheusMeterRegistry getPrometheusMeterRegistry() {
      return prometheusMeterRegistry;
    }

    @VisibleForTesting
    public static void initRegistry() {
      initRegistry(Optional.ofNullable(System.getenv(DREMIO_MICROMETER_HISTOGRAM_FLAVOR_ENV_VAR)));
    }

    @VisibleForTesting
    public static void initRegistry(Optional<String> oHistogramFlavor) {
      prometheusMeterRegistry =
          globalRegistry.getRegistries().stream()
              .filter(registry -> registry instanceof PrometheusMeterRegistry)
              .findFirst()
              .map(registry -> (PrometheusMeterRegistry) registry)
              .orElseGet(
                  () -> {
                    // OSS use case -- need to create a new PrometheusMeterRegistry and add it to
                    // the global registry
                    PrometheusMeterRegistry newPrometheusRegistry =
                        newPrometheusMeterRegistry(oHistogramFlavor);
                    newPrometheusRegistry.getPrometheusRegistry().register(new StandardExports());
                    globalRegistry.add(newPrometheusRegistry);

                    JmxMeterRegistry jmxMeterRegistry =
                        new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
                    jmxMeterRegistry.config().namingConvention(NamingConvention.identity);
                    globalRegistry.add(jmxMeterRegistry);

                    Stream.of(
                            new ClassLoaderMetrics(),
                            new JvmHeapPressureMetrics(),
                            new JvmMemoryMetrics(),
                            new JvmThreadMetrics(),
                            new JvmGcMetrics(),
                            new UptimeMetrics(),
                            new ProcessorMetrics(),
                            new FileDescriptorMetrics())
                        .forEach(binder -> binder.bindTo(globalRegistry));

                    return newPrometheusRegistry;
                  });

      // need codahale metric registry until we convert all clients to Micrometer
      codahaleMetricRegistry = new MetricRegistry();
      registerSysStats();

      CollectorRegistry collectorRegistry = prometheusMeterRegistry.getPrometheusRegistry();
      collectorRegistry.register(
          new DropwizardExports(
              RegistryHolder.codahaleMetricRegistry, newLegacyTagsSampleBuilder()));

      reporters = new ReporterManager(codahaleMetricRegistry);
    }

    @NotNull
    private static PrometheusMeterRegistry newPrometheusMeterRegistry(
        Optional<String> oHistogramFlavor) {
      final HistogramFlavor histogramFlavor =
          oHistogramFlavor
              .map(
                  v -> {
                    try {
                      return HistogramFlavor.valueOf(v);
                    } catch (IllegalArgumentException e) {
                      logger.warn(
                          "Invalid histogram flavor provided: {}. Defaulting to Prometheus.", v);
                      return HistogramFlavor.Prometheus;
                    }
                  })
              .orElse(HistogramFlavor.Prometheus);

      logger.info("Using histogram flavor: {}", histogramFlavor);

      return new PrometheusMeterRegistry(
          new PrometheusConfig() {
            @Override
            public String get(String key) {
              return null;
            }

            @Override
            public HistogramFlavor histogramFlavor() {
              return histogramFlavor;
            }
          });
    }

    @NotNull
    private static DefaultSampleBuilder newLegacyTagsSampleBuilder() {
      return new DefaultSampleBuilder() {
        @Override
        public Collector.MetricFamilySamples.Sample createSample(
            final String dropwizardName,
            final String nameSuffix,
            final List<String> additionalLabelNames,
            final List<String> additionalLabelValues,
            final double value) {
          if (Stream.of(GC, BUFFER_POOL, MEMORY, THREADS).noneMatch(dropwizardName::startsWith)) {
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
                dropwizardName, nameSuffix, additionalLabelNames, additionalLabelValues, value);
          }
        }
      };
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

  public static Meter unregister(Meter meter) {
    Meter removed = globalRegistry.remove(meter);
    if (removed != null) {
      logger.info("Removing old meter from globalRegistry. Meter ID: {}", meter.getId());
    }
    return removed;
  }

  public static Meter unregister(String meterName) {
    Meter meter = globalRegistry.find(meterName).meter();
    if (meter != null) {
      return unregister(meter);
    } else {
      logger.warn("Meter not found in globalRegistry. Meter name: {}", meterName);
    }
    return null;
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
    globalRegistry.forEachMeter(globalRegistry::remove);
    RegistryHolder.codahaleMetricRegistry.removeMatching((a, b) -> true);
    RegistryHolder.registerSysStats();
  }

  @VisibleForTesting
  public static Collection<MetricsConfigurator> getConfigurators() {
    return RegistryHolder.reporters.getParentConfigurators();
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

  public static String scrape() {
    return RegistryHolder.getPrometheusMeterRegistry().scrape();
  }
}
