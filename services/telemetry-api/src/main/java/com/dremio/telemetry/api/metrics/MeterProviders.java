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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;

/** Factory class to simplify creation of Micrometer meters. */
public final class MeterProviders {
  /**
   * Create a new MeterProvider<Counter> with the given name, description, and tags. When in use,
   * the Counter will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Counter
   * @param description the description of the Counter
   * @param tags the tags to apply to the Counter
   * @return a new MeterProvider<Counter>
   */
  public static MeterProvider<Counter> newCounterProvider(
      String name, String description, Iterable<Tag> tags) {
    return Counter.builder(name).description(description).tags(tags).withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<Counter> with the given name and description. When in use, the
   * Counter will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Counter
   * @param description the description of the Counter
   * @return a new MeterProvider<Counter>
   */
  public static MeterProvider<Counter> newCounterProvider(String name, String description) {
    return Counter.builder(name).description(description).withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<Counter> with the given name and tags. When in use, the Counter will
   * be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Counter
   * @param tags the tags to apply to the Counter
   * @return a new MeterProvider<Counter>
   */
  public static MeterProvider<Counter> newCounterProvider(String name, Iterable<Tag> tags) {
    return Counter.builder(name).tags(tags).withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<Counter> with the given name. When in use, the Counter will be
   * registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Counter
   * @return a new MeterProvider<Counter>
   */
  public static MeterProvider<Counter> newCounterProvider(String name) {
    return Counter.builder(name).withRegistry(globalRegistry);
  }

  /**
   * Create a new Supplier<Timer.ResourceSample> with the given name. When in use, the Timer will be
   * registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @return a new Supplier<Timer.ResourceSample>
   */
  public static Supplier<Timer.ResourceSample> newTimerResourceSampleSupplier(String name) {
    return () -> Timer.resource(globalRegistry, name);
  }

  /**
   * Create a new Supplier<Timer.ResourceSample> with the given name and tags. When in use, the
   * Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Supplier<Timer.ResourceSample>
   */
  public static Supplier<Timer.ResourceSample> newTimerResourceSampleSupplier(
      String name, Iterable<Tag> tags) {
    return () -> Timer.resource(globalRegistry, name).tags(tags);
  }

  /**
   * Create a new Supplier<Timer.ResourceSample> with the given name and description. When in use,
   * the Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @return a new Supplier<Timer.ResourceSample>
   */
  public static Supplier<Timer.ResourceSample> newTimerResourceSampleSupplier(
      String name, String description) {
    return () -> Timer.resource(globalRegistry, name).description(description);
  }

  /**
   * Create a new Timer.ResourceSample with the given name. When in use, the Timer will be
   * registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample newTimerResourceSample(String name) {
    return Timer.resource(globalRegistry, name);
  }

  /**
   * Create a new Timer.ResourceSample with the given name and description. When in use, the Timer
   * will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample newTimerResourceSample(String name, String description) {
    return Timer.resource(globalRegistry, name).description(description);
  }

  /**
   * Create a new Timer.ResourceSample with the given name, description, and tags. When in use, the
   * Timer will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @param tags the tags to apply to the Timer
   * @return a new Timer.ResourceSample
   */
  public static Timer.ResourceSample newTimerResourceSample(
      String name, String description, Iterable<Tag> tags) {
    return Timer.resource(globalRegistry, name).description(description).tags(tags);
  }

  /**
   * Create a new MeterProvider<Timer> with the given name. When in use, the Timer will be
   * registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @return a new MeterProvider<Timer>
   */
  public static MeterProvider<Timer> newTimerProvider(String name) {
    return Timer.builder(name).withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<Timer> with the given name and description. When in use, the Timer
   * will be registered with io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Timer
   * @param description the description of the Timer
   * @return a new MeterProvider<Timer>
   */
  public static MeterProvider<Timer> newTimerProvider(String name, String description) {
    return Timer.builder(name).description(description).withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<DistributionSummary> with the given name. When in use, the
   * DistributionSummary will be registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the DistributionSummary
   * @return a new MeterProvider<DistributionSummary>
   */
  public static MeterProvider<DistributionSummary> newDistributionSummaryProvider(String name) {
    return DistributionSummary.builder(name)
        .publishPercentileHistogram()
        .withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<DistributionSummary> with the given name and description. When in
   * use, the DistributionSummary will be registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the DistributionSummary
   * @param description the description of the DistributionSummary
   * @return a new MeterProvider<DistributionSummary>
   */
  public static MeterProvider<DistributionSummary> newDistributionSummaryProvider(
      String name, String description) {
    return DistributionSummary.builder(name)
        .description(description)
        .publishPercentileHistogram()
        .withRegistry(globalRegistry);
  }

  /**
   * Create a new MeterProvider<DistributionSummary> with the given name, description and tags. When
   * in use, the DistributionSummary will be registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the DistributionSummary
   * @param description the description of the DistributionSummary
   * @param tags the tags to apply to the distribution summary
   * @return a new MeterProvider<DistributionSummary>
   */
  public static MeterProvider<DistributionSummary> newDistributionSummaryProvider(
      String name, String description, Iterable<Tag> tags) {
    return DistributionSummary.builder(name)
        .description(description)
        .tags(tags)
        .publishPercentileHistogram()
        .withRegistry(globalRegistry);
  }

  /**
   * Create a new Gauge with the given name and function, registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Gauge
   * @param f a function that yields a double value for the gauge
   * @return a new Gauge
   */
  public static Gauge newGauge(String name, Supplier<Number> f) {
    return Gauge.builder(name, f).register(globalRegistry);
  }

  /**
   * Create a new Gauge with the given name, description and function, registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Gauge
   * @param description the description of the Gauge
   * @param f a function that yields a double value for the gauge
   * @return a new Gauge
   */
  public static Gauge newGauge(String name, String description, Supplier<Number> f) {
    return Gauge.builder(name, f).description(description).register(globalRegistry);
  }

  /**
   * Create a new Gauge with the given name, description, tags, and function, registered with
   * io.micrometer.core.instrument.Metrics.globalRegistry.
   *
   * @param name the name of the Gauge
   * @param description the description of the Gauge
   * @param tags the tags to apply to the Gauge
   * @param f a function that yields a double value for the gauge
   * @return a new Gauge
   */
  public static Gauge newGauge(
      String name, String description, Iterable<Tag> tags, Supplier<Number> f) {
    return Gauge.builder(name, f).description(description).tags(tags).register(globalRegistry);
  }

  private MeterProviders() {
    // private constructor to prevent instantiation
  }
}
