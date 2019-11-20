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
package com.dremio.telemetry.api.config;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;

/**
 * The core wrapping class for all metrics configurators. Capture common properties as well as reporter configuration to do
 * comparison, etc.
 */
public class MetricsConfigurator implements AutoCloseable {

  private final String name;
  private final String comment;
  private final ReporterConfigurator configurator;
  private final List<String> includes;
  private final List<String> excludes;
  private final MetricFilter filter;

  @JsonCreator
  public MetricsConfigurator(
      @JsonProperty("name") String name,
      @JsonProperty("comment") String comment,
      @JsonProperty("reporter") ReporterConfigurator reporter,
      @JsonProperty("includes") List<String> includes,
      @JsonProperty("excludes") List<String> excludes
      ) {
    super();
    this.name = Objects.requireNonNull(name, "All reporters must have a name");
    this.comment = comment;
    this.configurator = Objects.requireNonNull(reporter, () -> String.format("Invalid definition for reporter with name %s. Must define a reporter block.", name));
    this.includes = Optional.ofNullable(includes).orElse(Collections.emptyList());
    this.excludes = Optional.ofNullable(excludes).orElse(Collections.emptyList());

    if (includes.isEmpty() && excludes.isEmpty()) {
      filter = MetricFilter.ALL;
    } else {
      filter = new IncludesExcludesFilter(includes, excludes);
    }

  }

  public String getName() {
    return name;
  }

  public void start(MetricRegistry registry) {
    configurator.configureAndStart(name, registry, filter);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, comment, configurator, includes, excludes);
  }

  @Override
  public boolean equals(Object other) {
    if(other == null) {
      return false;
    }
    if(!this.getClass().equals(other.getClass())) {
      return false;
    }
    MetricsConfigurator o = (MetricsConfigurator) other;
    return Objects.equals(name, o.name)
        && Objects.equals(comment, o.comment)
        && Objects.equals(configurator, o.configurator)
        && Objects.equals(filter, o.filter);
  }

  @VisibleForTesting
  public ReporterConfigurator getConfigurator() {
    return configurator;
  }

  @Override
  public void close() {
    configurator.close();
  }
}
