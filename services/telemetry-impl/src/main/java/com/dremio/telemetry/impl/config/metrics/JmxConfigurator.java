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

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.dremio.telemetry.api.config.ConfigModule;
import com.dremio.telemetry.api.config.ReporterConfigurator;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Configurator for JMX
 */
@JsonTypeName("jmx")
public class JmxConfigurator extends ReporterConfigurator {

  private final TimeUnit rateUnit;
  private final TimeUnit durationUnit;
  private volatile JmxReporter reporter;

  @JsonCreator
  public JmxConfigurator(
      @JsonProperty("rate") TimeUnit rateUnit,
      @JsonProperty("duration") TimeUnit durationUnit) {
    super();
    this.rateUnit = Optional.ofNullable(rateUnit).orElse(TimeUnit.SECONDS);
    this.durationUnit = Optional.ofNullable(durationUnit).orElse(TimeUnit.MILLISECONDS);
  }

  @Override
  public void configureAndStart(String name, MetricRegistry registry, MetricFilter filter) {
    reporter = JmxReporter.forRegistry(registry).convertRatesTo(rateUnit).convertDurationsTo(durationUnit).filter(filter).build();
    reporter.start();
  }

  @Override
  public int hashCode() {
    return Objects.hash(rateUnit, durationUnit);
  }

  @Override
  public boolean equals(Object other) {
    if(other == null) {
      return false;
    }
    if(!other.getClass().equals(this.getClass())) {
      return false;
    }
    JmxConfigurator o = (JmxConfigurator) other;
    return Objects.equals(rateUnit, o.rateUnit)
        && Objects.equals(durationUnit, o.durationUnit);
  }

  @Override
  public void close() {
    if(reporter != null) {
      reporter.close();
    }
  }

  /**
   * Module that may be added to a jackson object mapper
   * so it can parse jmx config.
   */
  public static class Module extends ConfigModule {
    @Override
    public void setupModule(SetupContext context) {
      context.registerSubtypes(JmxConfigurator.class);
    }
  }
}
