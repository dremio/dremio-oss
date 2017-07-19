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
package com.dremio.common.util;

import java.sql.Date;
import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;
import org.joda.time.LocalDateTimes;

public class DateTimes {

  public static long toMillis(LocalDateTime localDateTime) {
    return LocalDateTimes.getLocalMillis(localDateTime);
  }

  public static long toMillis(DateTime dateTime) {
    return dateTime.toDateTime(DateTimeZone.UTC).getMillis();
  }

  public static int toMillisOfDay(final DateTime dateTime) {
    return dateTime.toDateTime(DateTimeZone.UTC).millisOfDay().get();
  }

  /**
   * Convert from JDBC date escape string format to utc millis, ignoring local
   * timezone.
   *
   * Note, the current implementation is ridiculous as it goes through two
   * conversions. Should be updated to no conversion.
   *
   * @param jdbcEscapeString
   * @return Milliseconds since epoch.
   */
  public static long toMillisFromJdbcDate(String jdbcEscapeString){
    return toMillis(new LocalDateTime(Date.valueOf(jdbcEscapeString).getTime()));
  }

  /**
   * Convert from JDBC timestamp escape string format to utc millis, ignoring local
   * timezone.
   *
   * Note, the current implementation is ridiculous as it goes through two
   * conversions. Should be updated to no conversion.
   *
   * @param jdbcEscapeString
   * @return Milliseconds since epoch.
   */
  public static long toMillisFromJdbcTimestamp(String jdbcEscapeString){
    return toMillis(new LocalDateTime(Timestamp.valueOf(jdbcEscapeString).getTime()));
  }

  public static DateTime toDateTime(LocalDateTime localDateTime) {
    return localDateTime.toDateTime(DateTimeZone.UTC);
  }
}
