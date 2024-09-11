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

import static com.dremio.telemetry.api.metrics.MeterProviders.newTimerProvider;

import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

/**
 * A simple Micrometer-based timer metric that can be updated to measure the duration of an
 * operation.
 */
public final class SimpleUpdatableTimer {
  private final MeterProvider<Timer> timer;

  public static SimpleUpdatableTimer of(String name) {
    return new SimpleUpdatableTimer(newTimerProvider(name));
  }

  public static SimpleUpdatableTimer of(String name, String description) {
    return new SimpleUpdatableTimer(newTimerProvider(name, description));
  }

  private SimpleUpdatableTimer(MeterProvider<Timer> timer) {
    this.timer = timer;
  }

  public void update(long duration, TimeUnit timeUnit) {
    timer.withTags().record(duration, timeUnit);
  }

  public void update(long duration, TimeUnit timeUnit, Iterable<Tag> tags) {
    timer.withTags(tags).record(duration, timeUnit);
  }
}
