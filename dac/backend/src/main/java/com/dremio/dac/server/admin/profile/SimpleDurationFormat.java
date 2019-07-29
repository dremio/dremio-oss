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
package com.dremio.dac.server.admin.profile;

import java.util.concurrent.TimeUnit;

/**
 * Representation of a millisecond duration in a human-readable format
 */
class SimpleDurationFormat {
  /**
   * @param durationInMillis
   */
  static String format(long durationInMillis) {
    final long days = TimeUnit.MILLISECONDS.toDays(durationInMillis);
    final long hours = TimeUnit.MILLISECONDS.toHours(durationInMillis) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(durationInMillis));
    final long minutes = TimeUnit.MILLISECONDS.toMinutes(durationInMillis) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(durationInMillis));
    final long seconds = TimeUnit.MILLISECONDS.toSeconds(durationInMillis) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(durationInMillis));
    final long milliSeconds = durationInMillis - TimeUnit.SECONDS.toMillis(TimeUnit.MILLISECONDS.toSeconds(durationInMillis));

    if (days >= 1) {
      return days + "d" + hours + "h" + minutes + "m";
    } else if (hours >= 1) {
      return hours + "h" + minutes + "m";
    } else if (minutes >= 1) {
      return minutes + "m" + seconds + "s";
    } else {
      return String.format("%.3fs", seconds + milliSeconds/1000.0);
    }
  }

  /**
   * @param durationInNanos
   */
  static String formatNanos(long durationInNanos) {

    if (durationInNanos < 1) {
      return "0s";
    }

    final long days = TimeUnit.NANOSECONDS.toDays(durationInNanos);
    final long hours = TimeUnit.NANOSECONDS.toHours(durationInNanos) - TimeUnit.DAYS.toHours(TimeUnit.NANOSECONDS.toDays(durationInNanos));
    final long minutes = TimeUnit.NANOSECONDS.toMinutes(durationInNanos) - TimeUnit.HOURS.toMinutes(TimeUnit.NANOSECONDS.toHours(durationInNanos));
    final long seconds = TimeUnit.NANOSECONDS.toSeconds(durationInNanos) - TimeUnit.MINUTES.toSeconds(TimeUnit.NANOSECONDS.toMinutes(durationInNanos));
    final long milliSeconds = TimeUnit.NANOSECONDS.toMillis(durationInNanos) - TimeUnit.SECONDS.toMillis(TimeUnit.NANOSECONDS.toSeconds(durationInNanos));
    final long microSeconds = TimeUnit.NANOSECONDS.toMicros(durationInNanos) - TimeUnit.MILLISECONDS.toMicros(TimeUnit.NANOSECONDS.toMillis(durationInNanos));
    final long nanoSeconds = durationInNanos - TimeUnit.MICROSECONDS.toNanos(TimeUnit.NANOSECONDS.toMicros(durationInNanos));

    if (days >= 1) {
      return days + "d" + hours + "h" + minutes + "m";
    } else if (hours >= 1) {
      return hours + "h" + minutes + "m";
    } else if (minutes >= 1) {
      return minutes + "m" + seconds + "s";
    } else if (seconds >= 1) {
      return String.format("%.3fs", seconds + milliSeconds/1000.0);
    } else if (milliSeconds >= 1) {
      return milliSeconds + "ms";
    } else if (microSeconds >= 1) {
      return microSeconds + "us";
    } else {
      return nanoSeconds + "ns";
    }
  }
}
