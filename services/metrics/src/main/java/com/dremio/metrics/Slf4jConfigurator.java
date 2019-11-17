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
package com.dremio.metrics;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;


/**
 * Configurator for SLF4j Logging
 */
@JsonTypeName("slf4j")
public class Slf4jConfigurator extends ReporterConfigurator {

  private final long intervalMS;
  private final String loggerName;
  private final TimeUnit rateUnit;
  private final TimeUnit durationUnit;
  private volatile Slf4jReporter reporter;

  @JsonCreator
  public Slf4jConfigurator(
      @JsonProperty("logger") String loggerName,
      @JsonProperty("rate") TimeUnit rateUnit,
      @JsonProperty("duration") TimeUnit durationUnit,
      @JsonProperty("intervalMS") Long intervalMS) {
    super();
    this.loggerName = Optional.ofNullable(loggerName).orElse("metrics");
    this.rateUnit = Optional.ofNullable(rateUnit).orElse(TimeUnit.SECONDS);
    this.durationUnit = Optional.ofNullable(durationUnit).orElse(TimeUnit.MILLISECONDS);
    this.intervalMS = Optional.ofNullable(intervalMS).orElse(120_000L);
  }

  @Override
  public void configureAndStart(String name, MetricRegistry registry, MetricFilter filter) {
    reporter = Slf4jReporter.forRegistry(registry)
        .outputTo(LoggerFactory.getLogger(loggerName))
        .convertRatesTo(rateUnit)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build();
    reporter.start(intervalMS, TimeUnit.MILLISECONDS);
  }

  @Override
  public int hashCode() {
    return Objects.hash(loggerName, rateUnit, durationUnit, intervalMS);
  }

  @Override
  public boolean equals(Object other) {
    if(other == null) {
      return false;
    }
    if(!other.getClass().equals(this.getClass())) {
      return false;
    }
    Slf4jConfigurator o = (Slf4jConfigurator) other;
    return Objects.equals(loggerName, o.loggerName)
        && Objects.equals(rateUnit, o.rateUnit)
        && Objects.equals(durationUnit, o.durationUnit)
        && Objects.equals(intervalMS, o.intervalMS);
  }

  @Override
  public void close() {
    if(reporter != null) {
      reporter.close();
    }
  }
}
