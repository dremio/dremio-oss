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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/** Utils for conversion across objects */
public final class DateTimeConversionUtils {

  // RFC3339 us used by CEL-Java as well as Nessie
  public static final DateTimeFormatter RFC_3339_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendValue(ChronoField.YEAR, 4, 5, SignStyle.EXCEEDS_PAD)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          .appendLiteral('T')
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          .appendOffset("+HH:MM", "Z")
          .toFormatter();

  /**
   * Returns RFC_3339 formatted string with precisions up to seconds from a proto time object.
   *
   * @param protoTime
   * @return
   */
  public static String toRfc3339FormattedStr(com.google.protobuf.Timestamp protoTime) {
    Instant checkStartTime = Instant.ofEpochSecond(protoTime.getSeconds());
    return RFC_3339_FORMATTER.format(ZonedDateTime.ofInstant(checkStartTime, ZoneId.of("UTC")));
  }

  /**
   * Returns {@link OffsetDateTime} object with a precision at the seconds level
   *
   * @param protoTime
   * @return
   */
  @Nullable
  public static OffsetDateTime toOffsetDateTime(com.google.protobuf.Timestamp protoTime) {
    if (protoTime == null) {
      return null;
    }
    Instant instant = Instant.ofEpochSecond(protoTime.getSeconds());
    return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  /**
   * Returns {@link OffsetDateTime} object with a precision at the seconds level
   *
   * @param inputDateTime
   * @return
   */
  @Nullable
  public static OffsetDateTime toOffsetDateTime(String inputDateTime) {
    if (StringUtils.isEmpty(inputDateTime)) {
      return null;
    }

    return OffsetDateTime.parse(inputDateTime, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
  }

  /**
   * Returns {@link OffsetDateTime} object with a precision at the milliseconds level
   *
   * @param inputTimeStampAsMilli
   * @return
   */
  public static OffsetDateTime toOffsetDateTime(long inputTimeStampAsMilli) {
    Instant instant = Instant.ofEpochMilli(inputTimeStampAsMilli);
    return OffsetDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  /**
   * Returns {@link com.google.protobuf.Timestamp} with a precision at the seconds level
   *
   * @param time
   * @return
   */
  @Nullable
  public static com.google.protobuf.Timestamp toProtoTime(OffsetDateTime time) {
    if (time == null) {
      return null;
    }
    long epochSeconds = time.toEpochSecond();
    return com.google.protobuf.Timestamp.newBuilder().setSeconds(epochSeconds).build();
  }

  /**
   * Convert {@link OffsetDateTime} timestamp to milliseconds
   *
   * @param time
   * @return timestamp in milliseconds
   */
  public static long toTimestampAsMilliseconds(OffsetDateTime time) {
    return time.toInstant().toEpochMilli();
  }

  private DateTimeConversionUtils() {
    // Not to be instantiated
  }
}
