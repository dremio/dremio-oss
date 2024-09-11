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

import static com.dremio.telemetry.api.metrics.MeterProviders.newDistributionSummaryProvider;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;

/** A simple Micrometer-based distribution summary metric that can be recorded. */
public final class SimpleDistributionSummary {
  private final MeterProvider<DistributionSummary> distributionSummary;

  public static SimpleDistributionSummary of(String name) {
    return new SimpleDistributionSummary(newDistributionSummaryProvider(name));
  }

  public static SimpleDistributionSummary of(String name, String description) {
    return new SimpleDistributionSummary(newDistributionSummaryProvider(name, description));
  }

  public static SimpleDistributionSummary of(String name, String description, Iterable<Tag> tags) {
    return new SimpleDistributionSummary(newDistributionSummaryProvider(name, description, tags));
  }

  private SimpleDistributionSummary(MeterProvider<DistributionSummary> distributionSummary) {
    this.distributionSummary = distributionSummary;
  }

  public void recordAmount(double amount) {
    distributionSummary.withTags().record(amount);
  }
}
