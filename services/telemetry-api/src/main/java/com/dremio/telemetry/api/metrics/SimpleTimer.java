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

import static com.dremio.telemetry.api.metrics.MeterProviders.newTimerResourceSampleSupplier;

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.util.function.Supplier;

/**
 * A simple Micrometer-based timer metric that can be used to measure the duration of a block of
 * code.
 */
public final class SimpleTimer {
  private final Supplier<Timer.ResourceSample> timer;

  public static SimpleTimer of(String name) {
    return new SimpleTimer(newTimerResourceSampleSupplier(name));
  }

  public static SimpleTimer of(String name, Iterable<Tag> tags) {
    return new SimpleTimer(newTimerResourceSampleSupplier(name, tags));
  }

  public static SimpleTimer of(String name, String description) {
    return new SimpleTimer(newTimerResourceSampleSupplier(name, description));
  }

  private SimpleTimer(Supplier<Timer.ResourceSample> timer) {
    this.timer = timer;
  }

  public Timer.ResourceSample start() {
    return timer.get();
  }
}
