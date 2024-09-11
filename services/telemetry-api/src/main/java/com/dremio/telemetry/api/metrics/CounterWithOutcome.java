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

import static com.dremio.telemetry.api.metrics.CommonTags.TAGS_OUTCOME_ERROR;
import static com.dremio.telemetry.api.metrics.CommonTags.TAGS_OUTCOME_SUCCESS;
import static com.dremio.telemetry.api.metrics.CommonTags.TAGS_OUTCOME_USER_ERROR;
import static com.dremio.telemetry.api.metrics.MeterProviders.newCounterProvider;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;

/**
 * Represents a Micrometer counter for the same general metric with tags for successful and errored
 * outcomes
 */
public final class CounterWithOutcome {
  private final MeterProvider<Counter> counter;

  public static CounterWithOutcome of(String name) {
    return new CounterWithOutcome(newCounterProvider(name));
  }

  public static CounterWithOutcome of(String name, Iterable<Tag> tags) {
    return new CounterWithOutcome(newCounterProvider(name, tags));
  }

  public static CounterWithOutcome of(String name, String description) {
    return new CounterWithOutcome(newCounterProvider(name, description));
  }

  public static CounterWithOutcome of(String name, String description, Iterable<Tag> tags) {
    return new CounterWithOutcome(newCounterProvider(name, description, tags));
  }

  private CounterWithOutcome(MeterProvider<Counter> counter) {
    this.counter = counter;
  }

  public void succeeded() {
    counter.withTags(TAGS_OUTCOME_SUCCESS).increment();
  }

  public long countSucceeded() {
    return (long) counter.withTags(TAGS_OUTCOME_SUCCESS).count();
  }

  public void errored() {
    counter.withTags(TAGS_OUTCOME_ERROR).increment();
  }

  public long countErrored() {
    return (long) counter.withTags(TAGS_OUTCOME_ERROR).count();
  }

  public void userErrored() {
    counter.withTags(TAGS_OUTCOME_USER_ERROR).increment();
  }

  public long countUserErrored() {
    return (long) counter.withTags(TAGS_OUTCOME_USER_ERROR).count();
  }
}
