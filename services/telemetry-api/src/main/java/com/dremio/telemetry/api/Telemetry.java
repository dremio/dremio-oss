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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.ServiceLoader;
import java.util.function.Consumer;

import javax.inject.Provider;

import com.dremio.telemetry.api.config.AutoRefreshConfigurator;
import com.dremio.telemetry.api.config.AutoRefreshConfigurator.CompleteRefreshConfig;
import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.MetricsConfigurator;
import com.dremio.telemetry.api.config.TelemetryConfigurator;
import com.dremio.telemetry.api.config.TracerConfigurator;
import com.dremio.telemetry.api.metrics.Metrics;
import com.dremio.telemetry.utils.TracerFacade;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

import io.opentelemetry.api.common.AttributeKey;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;

/**
 * Owner to all telemetry utilities.
 */
public final class Telemetry {
  private static final String CONFIG_FILE = "dremio-telemetry.yaml";
  private static final String CONFIG_FILE_PROPERTY = "dremio.telemetry.configfile";
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Telemetry.class);

  private static AutoRefreshConfigurator<InnerTelemetryConf> CONFIG_REFRESHER;
  private static InnerTelemetryConfigListener CONFIG_REFRESH_LISTENER;

  /** Sampling is forced if this Attribute exists in the Span */
  public static final AttributeKey<Boolean> FORCE_SAMPLING_ATTRIBUTE = AttributeKey.booleanKey("dremio-sample");

  /**
   * Reads the telemetry config file and sets itself up to listen for changes.
   * If telemetry is already available, the most up to date telemetry dependencies (e.g. tracer)
   * will be added to the registry.
   */
  @SuppressWarnings("unchecked")
  public static void startTelemetry() {

    if (CONFIG_REFRESH_LISTENER != null) {
      // No need to restart.
      return;
    }

    final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    ImmutableList.Builder<ConfigModule> modulesBuilder = ImmutableList.builder();
    ServiceLoader.load(ConfigModule.class).forEach(modulesBuilder::add);
    ImmutableList<ConfigModule> modules = modulesBuilder.build();

    if (modules.isEmpty()) {
      logger.warn("Unable to discover any modules for telemetry config. Will not refresh config.");
      return;
    }
    mapper.registerModules(modules);

    final ObjectReader reader = mapper.readerFor(TelemetryConfigurator.class);

    Provider<CompleteRefreshConfig<InnerTelemetryConf>> configurationProvider =
      new Provider<CompleteRefreshConfig<InnerTelemetryConf>>() {

        private final ExceptionWatcher exceptionWatcher =
          new ExceptionWatcher((ex) -> logger.warn("Failure reading telemetry configuration. Leaving telemetry as is.", ex));

        @Override
        public CompleteRefreshConfig<InnerTelemetryConf> get() {
          URL resource = null;
          CompleteRefreshConfig<InnerTelemetryConf> ret = null;
          try {
            String configFilePath = System.getProperty(CONFIG_FILE_PROPERTY);
            if (configFilePath!=null && !configFilePath.isEmpty()) {
              File file = new File(configFilePath);
              if (file.exists()) {
                resource = file.toURI().toURL();
              }
            }
            if (resource == null) {
              resource = Resources.getResource(CONFIG_FILE);
            }

            final TelemetryConfigurator fromConfig = reader.readValue(resource);

            final InnerTelemetryConf telemConf = new InnerTelemetryConf(fromConfig.getMetricsConfigs(), fromConfig.getTracerConfig());

            ret = new CompleteRefreshConfig<>(fromConfig.getRefreshConfig(), telemConf);
            exceptionWatcher.reset();
          } catch (IllegalArgumentException | IOException ex) {
            exceptionWatcher.notify(ex);
          }
          return ret;
        }
      };

    CONFIG_REFRESH_LISTENER = new InnerTelemetryConfigListener(TracerFacade.INSTANCE);
    CONFIG_REFRESHER = new AutoRefreshConfigurator<>(configurationProvider, CONFIG_REFRESH_LISTENER);

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

  private static class InnerTelemetryConf {
    private final Collection<MetricsConfigurator> metricsConf;
    private final TracerConfigurator tracerConf;

    InnerTelemetryConf(Collection<MetricsConfigurator> metricsConfigurators, TracerConfigurator tracerConf) {
      this.metricsConf = metricsConfigurators;
      this.tracerConf = tracerConf;
    }

    public Collection<MetricsConfigurator> getMetricsConf() {
      return metricsConf;
    }

    public TracerConfigurator getTracerConf() {
      return tracerConf;
    }
  }

  /**
   * Listens to changes on telemetry config. Does not receive updates regarding refresh setting changes.
   */
  static class InnerTelemetryConfigListener implements Consumer<InnerTelemetryConf> {
    private final AutoRefreshConfigurator.ValueChangeDetector<Collection<MetricsConfigurator>> rememberedMetrics;
    private final AutoRefreshConfigurator.ValueChangeDetector<TracerConfigurator> rememberedTracer;

    InnerTelemetryConfigListener(TracerFacade tracerFacade) {
      rememberedMetrics = configChangeConsumer("metrics", Metrics::onChange);
      rememberedTracer = configChangeConsumer("tracer",
          (tracerConf) -> {
            final Tracer newTracer = tracerConf == null ? NoopTracerFactory.create() : tracerConf.getTracer();
            tracerFacade.setTracer(newTracer);
          });
    }

    private static <T> AutoRefreshConfigurator.ValueChangeDetector<T> configChangeConsumer(String type, Consumer<T> consumer) {
      return new AutoRefreshConfigurator.ValueChangeDetector<>((t) -> {
        logger.debug("Updating {} configuration", type);
        consumer.accept(t);
        logger.debug("Updated {} configuration", type);
      });
    }

    @Override
    public synchronized void accept(InnerTelemetryConf update) {
      Collection<MetricsConfigurator> newMetrics = null;
      TracerConfigurator newTrace = null;

      if (update != null) {
        newMetrics = update.getMetricsConf();
        newTrace = update.getTracerConf();
      }

      rememberedMetrics.checkNewValue(newMetrics);
      rememberedTracer.checkNewValue(newTrace);
    }
  }
}
