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
package com.dremio.telemetry.impl.config.metrics;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.ReporterConfigurator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;

import io.opencensus.contrib.dropwizard.DropWizardMetrics;
import io.opencensus.exporter.stats.stackdriver.StackdriverStatsExporter;
import io.opencensus.metrics.Metrics;
import io.opencensus.metrics.export.MetricProducer;

/**
 * Configurator for metrics stackdriver backend.
 * Exports metrics to OpenCensus and then through a StackDriver exporter.
 */
@JsonTypeName("stackdriver")
public class StackDriverConfigurator extends ReporterConfigurator {

  private MetricProducer producer;
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StackDriverConfigurator.class);

  @JsonCreator
  public StackDriverConfigurator() {
    super();
  }

  @Override
  public void configureAndStart(String name, MetricRegistry registry, MetricFilter filter) {
    this.producer = new DropWizardMetrics(Collections.singletonList(registry), filter);

    Metrics.getExportComponent().getMetricProducerManager().add(producer);

    try {
      StackdriverStatsExporter.createAndRegister();
    } catch (IOException e) {
      logger.warn("Could not setup stackdriver stats", e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    return other instanceof StackDriverConfigurator;
  }

  @Override
  public void close() {
    Metrics.getExportComponent().getMetricProducerManager().remove(producer);
    StackdriverStatsExporter.unregister();
  }

  /**
   * Module that may be added to a jackson object mapper
   * so it can parse StackDriver config.
   */
  public static class Module extends ConfigModule {
    @Override
    public void setupModule(SetupContext context) {
      context.registerSubtypes(StackDriverConfigurator.class);
    }
  }
}
