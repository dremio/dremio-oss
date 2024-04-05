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
package com.dremio.service.accelerator;

import com.dremio.service.namespace.proto.TimePeriod;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/** Collection of helper methods for the accelerator */
public final class AccelerationUtils {
  private AccelerationUtils() {}

  public static long toMillis(final TimePeriod period) {
    final long duration = period.getDuration();
    final TimePeriod.TimeUnit unit = period.getUnit();
    switch (unit) {
      case SECONDS:
        return TimeUnit.SECONDS.toMillis(duration);
      case MINUTES:
        return TimeUnit.MINUTES.toMillis(duration);
      case HOURS:
        return TimeUnit.HOURS.toMillis(duration);
      case DAYS:
        return TimeUnit.DAYS.toMillis(duration);
      case WEEKS:
        return TimeUnit.DAYS.toMillis(7 * duration);
      case MONTHS:
        return TimeUnit.DAYS.toMillis(30 * duration);
      default:
        throw new UnsupportedOperationException(String.format("unsupported unit: %s", unit));
    }
  }

  public static <T> Stream<T> selfOrEmpty(final Stream<T> stream) {
    if (stream == null) {
      return Stream.of();
    }
    return stream;
  }

  /** Returns the iterable as list if not null or an empty list. */
  public static <T> List<T> selfOrEmpty(final List<T> iterable) {
    if (iterable == null) {
      return ImmutableList.of();
    }
    return iterable;
  }
}
