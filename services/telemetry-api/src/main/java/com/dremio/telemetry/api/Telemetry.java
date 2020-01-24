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

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import javax.inject.Provider;

import com.dremio.common.collections.Tuple;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.service.SingletonRegistry;
import com.dremio.telemetry.api.config.AutoRefreshConfigurator;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.dremio.telemetry.api.config.RefreshConfiguration;
import com.dremio.telemetry.api.config.ReporterConfigurator;
import com.dremio.telemetry.api.config.TelemetryConfigurator;
import com.dremio.telemetry.api.config.TracerConfigurator;
import com.dremio.telemetry.api.metrics.Metrics;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.Resources;

import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;

/**
 * Owner to all telemetry utilities.
 */
public final class Telemetry {
  private static final String CONFIG_FILE = "dremio-telemetry.yaml";
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Telemetry.class);

  // The refresher just refreshes our Metrics Config (That's the only thing in the telemetryConfig we care about).
  private static AutoRefreshConfigurator<Tuple<Collection<MetricsConfigurator>, TracerConfigurator>> configRefresher;
  private static TelemetryConfigListener configRefreshListener;

  /**
   * Reads the telemetry config file and sets itself up to listen for changes.
   * If telemetry is already available, the most up to date telemetry dependencies (e.g. tracer)
   * will be added to the registry.
   */
  @SuppressWarnings("unchecked")
  public static void startTelemetry(ScanResult scan, SingletonRegistry bootstrapRegistry) {

    if (configRefreshListener != null) {
      // Update the new registry with the present tracer.
      // No need to restart.
      configRefreshListener.addRegistry(bootstrapRegistry);
      return;
    }

    final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    Collection<Class<? extends ReporterConfigurator>> metricsConfigurators = scan.getImplementations(ReporterConfigurator.class);
    Collection<Class<? extends TracerConfigurator>> tracerConfigurators = scan.getImplementations(TracerConfigurator.class);

    mapper.registerSubtypes((Collection<Class<?>>) (Object) metricsConfigurators);
    mapper.registerSubtypes((Collection<Class<?>>) (Object) tracerConfigurators);

    final ObjectReader reader = mapper.readerFor(TelemetryConfigurator.class);

    Provider<Tuple<RefreshConfiguration, Tuple<Collection<MetricsConfigurator>, TracerConfigurator>>> configurationProvider =
      new Provider<Tuple<RefreshConfiguration, Tuple<Collection<MetricsConfigurator>, TracerConfigurator>>>() {

        private final ExceptionWatcher exceptionWatcher =
          new ExceptionWatcher((ex) -> logger.warn("Failure reading telemetry configuration. Leaving telemetry as is.", ex));

        @Override
        public Tuple<RefreshConfiguration, Tuple<Collection<MetricsConfigurator>, TracerConfigurator>> get() {
          final URL resource;
          Tuple<RefreshConfiguration, Tuple<Collection<MetricsConfigurator>, TracerConfigurator>> ret = null;
          try {
            resource = Resources.getResource(CONFIG_FILE);
            final TelemetryConfigurator fromConfig = reader.readValue(resource);

            ret = Tuple.of(fromConfig.getRefreshConfig(), Tuple.of(fromConfig.getMetricsConfigs(), fromConfig.getTracerConfig()));
            exceptionWatcher.reset();
          } catch (IllegalArgumentException | IOException ex) {
            exceptionWatcher.notify(ex);
          }
          return ret;
        }
      };

    configRefreshListener = new TelemetryConfigListener(bootstrapRegistry);
    configRefresher = new AutoRefreshConfigurator<>(configurationProvider, configRefreshListener);

  }

  /**
   * Only invokes exceptionConsumer when the exception message is different than the previous message.
   */
  static class ExceptionWatcher {
    private final Consumer<Exception> consumer;
    private String lastExceptionMessage;

    ExceptionWatcher(Consumer<Exception> exceptionConsumer) {
      this.consumer = exceptionConsumer;
    }

    void reset() {
      lastExceptionMessage = "";
    }

    void notify(Exception ex) {
      if (!ex.getMessage().equals(lastExceptionMessage)) {
        lastExceptionMessage = ex.getMessage();
        consumer.accept(ex);
      }
    }
  }

  /**
   * Listens to changes on telemetry config.
   */
  static class TelemetryConfigListener implements Consumer<Tuple<Collection<MetricsConfigurator>, TracerConfigurator>> {
    private AutoRefreshConfigurator.ValueChangeDetector<Collection<MetricsConfigurator>> rememberedMetrics;
    private AutoRefreshConfigurator.ValueChangeDetector<TracerConfigurator> rememberedTracer;

    private volatile List<SingletonRegistry> registries = new ArrayList<>();

    TelemetryConfigListener(SingletonRegistry bootstrapRegistry) {
      this.registries.add(bootstrapRegistry);
      bootstrapRegistry.bind(Tracer.class, NoopTracerFactory.create());

      rememberedMetrics = new AutoRefreshConfigurator.ValueChangeDetector<>(Metrics::onChange);
      rememberedTracer = new AutoRefreshConfigurator.ValueChangeDetector<>(this::onTracerConfChange);
    }

    @Override
    public synchronized void accept(Tuple<Collection<MetricsConfigurator>, TracerConfigurator> update) {
      Collection<MetricsConfigurator> newMetrics = null;
      TracerConfigurator newTrace = null;

      if (update != null) {
        newMetrics = update.first;
        newTrace = update.second;
      }

      rememberedMetrics.checkNewValue(newMetrics);
      rememberedTracer.checkNewValue(newTrace);
    }

    private synchronized void onTracerConfChange(TracerConfigurator tracerConf) {
      final Tracer newTracer = tracerConf == null ? NoopTracerFactory.create() : tracerConf.getTracer();
      registries.forEach((reg) -> reg.replace(Tracer.class, newTracer));
    }

    /**
     * Allows multiple registries to depend on the same config file.
     * @param registry
     */
    private synchronized void addRegistry(SingletonRegistry registry) {
      final TracerConfigurator traceConf = rememberedTracer.getLastValue();
      registry.bind(Tracer.class, traceConf == null ? NoopTracerFactory.create() : traceConf.getTracer());
    }

  }
}
