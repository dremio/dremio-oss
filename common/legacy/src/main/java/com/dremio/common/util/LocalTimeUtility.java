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

package com.dremio.common.util;

import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class LocalTimeUtility {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(LocalTimeUtility.class);
  private static final String TIME_MILLI_FORMAT = "HH:mm:ss.SSS";
  private static final String UTC_ZONE = "UTC";

  public static DateTimeFormatter getFormaterForTime() {
    return DateTimeFormatter.ofPattern(TIME_MILLI_FORMAT);
  }

  public static LocalTime getTimeFromMillis(long value) {
    return getTimeFromMillis(value, ZoneId.of(UTC_ZONE));
  }

  public static ZoneId getTimeZoneId() {
    String timezone = System.getProperty("user.timezone");
    try {
      return ZoneId.of(timezone);
    } catch (Exception e) {
      logger.error("'" + timezone + "' is an unregistered timezone. Using UTC");
      return ZoneId.of(UTC_ZONE);
    }
  }

  public static LocalTime getTimeFromMillis(long value, ZoneId zone) {
    return java.time.Instant.ofEpochMilli(value).atZone(zone).toLocalTime();
  }

  public static int getMillisFromTime(LocalTime time) {
    return (int) (time.toNanoOfDay() / 1_000_000);
  }

  public static int getMillis(LocalTime time) {
    return time.getNano() / 1_000_000;
  }
}
