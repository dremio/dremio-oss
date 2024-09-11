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

import static com.dremio.telemetry.api.metrics.MeterProviders.newCounterProvider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;

/** A simple Micrometer-based counter metric that can be incremented. */
public final class SimpleCounter {
  private final MeterProvider<Counter> counter;

  public static SimpleCounter of(String name) {
    return new SimpleCounter(newCounterProvider(name));
  }

  public static SimpleCounter of(String name, Iterable<Tag> tags) {
    return new SimpleCounter(newCounterProvider(name, tags));
  }

  public static SimpleCounter of(String name, String description) {
    return new SimpleCounter(newCounterProvider(name, description));
  }

  public static SimpleCounter of(String name, String description, Iterable<Tag> tags) {
    return new SimpleCounter(newCounterProvider(name, description, tags));
  }

  private SimpleCounter(MeterProvider<Counter> counter) {
    this.counter = counter;
  }

  public void increment() {
    counter.withTags().increment();
  }

  public void increment(double amount) {
    counter.withTags().increment(amount);
  }

  public long count() {
    return (long) counter.withTags().count();
  }

  @Override
  public String toString() {
    return Long.toString(count());
  }
}
