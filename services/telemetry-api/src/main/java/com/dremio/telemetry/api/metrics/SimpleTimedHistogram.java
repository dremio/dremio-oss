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

import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Timer.ResourceSample;
import java.util.function.Supplier;

/**
 * A simple Micrometer-based timer metric that publishes percentiles based on an underlying
 * histogram. Can be used to measure latencies as percentile distributions.
 */
public final class SimpleTimedHistogram {
  private final Supplier<ResourceSample> timer;

  public static SimpleTimedHistogram of(String name, String description, Iterable<Tag> tags) {
    return new SimpleTimedHistogram(() -> TimerUtils.timedHistogram(name, description, tags));
  }

  public static SimpleTimedHistogram of(String name, String description) {
    return new SimpleTimedHistogram(() -> TimerUtils.timedHistogram(name, description));
  }

  private SimpleTimedHistogram(Supplier<io.micrometer.core.instrument.Timer.ResourceSample> timer) {
    this.timer = timer;
  }

  public Timer.ResourceSample start() {
    return timer.get();
  }
}
