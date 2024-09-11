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

package com.dremio.services.nodemetrics.persistence;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAmount;

class TimeUtils {
  /** True if the first Instant is before or precisely equals the second Instant, otherwise false */
  static boolean firstIsBeforeOrEqualsSecond(Instant first, Instant second) {
    return first.equals(second) || first.isBefore(second);
  }

  /** True if the first Instant is after or precisely equals the second Instant, otherwise false */
  static boolean firstIsAfterOrEqualsSecond(Instant first, Instant second) {
    return first.equals(second) || first.isAfter(second);
  }

  /**
   * Gets the current Instant at second-granularity. Useful if you need to compare to another
   * Instant at second precision
   */
  static Instant getNowSeconds() {
    return Instant.now().truncatedTo(ChronoUnit.SECONDS);
  }

  static String getNodeMetricsFilename(TemporalAmount timeAgo) {
    String fileStem =
        new SecondPrecisionDateTimeFormatter().formatDateTime(Instant.now().minus(timeAgo));
    return String.format("%s.csv", fileStem);
  }

  static String getNodeMetricsFilename(TemporalAmount timeAgo, CompactionType compactionType) {
    String fileStem =
        new SecondPrecisionDateTimeFormatter().formatDateTime(Instant.now().minus(timeAgo));
    String prefix;
    switch (compactionType) {
      case Uncompacted:
        throw new IllegalArgumentException();
      case Single:
        prefix = "s";
        break;
      case Double:
        prefix = "d";
        break;
      default:
        throw new IllegalStateException();
    }
    return String.format("%s_%s.csv", prefix, fileStem);
  }
}
