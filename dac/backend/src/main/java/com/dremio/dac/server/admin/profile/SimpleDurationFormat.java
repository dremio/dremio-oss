/*
 * Copyright (C) 2017 Dremio Corporation
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
}
