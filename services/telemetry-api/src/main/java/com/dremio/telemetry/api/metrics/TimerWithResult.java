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

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;

public class TimerWithResult {
  private final MeterRegistry registry;
  private final String name;
  private final String description;
  private final Tags tags;

  public TimerWithResult(MeterRegistry registry, String name, String description, Tags tags) {
    this.registry = registry;
    this.name = name;
    this.description = description;
    this.tags = tags;
  }

  public TimerWithResult withNewTags(Tags newTags) {
    return new TimerWithResult(registry, name, description, newTags);
  }

  public TimerWithResult withAddedTags(Tags addedTags) {
    return new TimerWithResult(registry, name, description, tags.and(addedTags));
  }

  public Timer getTimer() {
    return Timer.builder(name).description(description).tags(tags).register(registry);
  }

  void record(Runnable runnable) {
    Clock clock = registry.config().clock();
    long start = clock.monotonicTime();
    String result = CommonTags.TAG_RESULT_VALUE_SUCCESS;

    try {
      runnable.run();
    } catch (Throwable t) {
      result = CommonTags.TAG_RESULT_VALUE_FAILURE;
    } finally {
      Timer timer =
          Timer.builder(name)
              .description(description)
              .tags(tags.and(CommonTags.TAG_RESULT_KEY, result))
              .register(registry);

      timer.record(clock.monotonicTime() - start, NANOSECONDS);
    }
  }
}
