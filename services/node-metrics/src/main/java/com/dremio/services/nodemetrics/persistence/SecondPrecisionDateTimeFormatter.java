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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/** Supports conversions between a datetime and a String representation at second precision. */
final class SecondPrecisionDateTimeFormatter implements NodeMetricsDateTimeFormatter {
  // Technically we could format date times with any timezone, but UTC makes it simplest to avoid
  // needing to store a timezone offset in persisted filenames
  private static final ZoneId TIMEZONE = ZoneId.of("UTC");

  /** ISO 8601-like but with only filename safe characters (no colons or plus) */
  private static final DateTimeFormatter DATETIME_FORMATTER =
      DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'");

  @Override
  public String formatDateTime(Instant instant) {
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, TIMEZONE);
    return DATETIME_FORMATTER.format(dateTime);
  }

  @Override
  public String getDateTimeGlob() {
    // This needs to correspond to the DATETIME_FORMATTER format
    String digitGlob = "[0-9]";
    return digitGlob.repeat(8) + "T" + digitGlob.repeat(6) + "Z";
  }

  @Override
  public ZonedDateTime parseDateTime(String dateTime) {
    LocalDateTime localDateTime = LocalDateTime.parse(dateTime, DATETIME_FORMATTER);
    return localDateTime.atZone(TIMEZONE);
  }
}
