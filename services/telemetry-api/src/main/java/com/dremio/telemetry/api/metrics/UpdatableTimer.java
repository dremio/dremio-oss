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

import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_KEY;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_EXCEPTION_NONE;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_ERROR;
import static com.dremio.telemetry.api.metrics.CommonTags.TAG_OUTCOME_SUCCESS;
import static com.dremio.telemetry.api.metrics.MeterProviders.newTimerProvider;

import io.micrometer.core.instrument.Meter.MeterProvider;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.TimeUnit;

/**
 * A Micrometer-based timer metric that can be updated to measure the duration of an operation and
 * tagged with the "outcome" tag for "success" or "error".
 */
public final class UpdatableTimer {
  private final MeterProvider<Timer> timer;

  public static UpdatableTimer of(String name) {
    return new UpdatableTimer(newTimerProvider(name));
  }

  public static UpdatableTimer of(String name, String description) {
    return new UpdatableTimer(newTimerProvider(name, description));
  }

  private UpdatableTimer(MeterProvider<Timer> timer) {
    this.timer = timer;
  }

  public void updateOnSuccess(long duration, TimeUnit timeUnit) {
    timer.withTags(Tags.of(TAG_OUTCOME_SUCCESS, TAG_EXCEPTION_NONE)).record(duration, timeUnit);
  }

  public void updateOnError(long duration, TimeUnit timeUnit) {
    timer.withTags(Tags.of(TAG_OUTCOME_ERROR, TAG_EXCEPTION_NONE)).record(duration, timeUnit);
  }

  public void updateOnError(long duration, TimeUnit timeUnit, Throwable ex) {
    updateOnError(duration, timeUnit, ex.getClass());
  }

  public void updateOnError(long duration, TimeUnit timeUnit, Class<? extends Throwable> exClass) {
    timer
        .withTags(Tags.of(TAG_OUTCOME_ERROR, Tag.of(TAG_EXCEPTION_KEY, exClass.getSimpleName())))
        .record(duration, timeUnit);
  }
}
