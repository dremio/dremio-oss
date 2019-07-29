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
package com.dremio.exec.expr.fn.impl;

import java.text.ParseException;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.chrono.DayOfWeekFromSundayChronology;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.fn.JodaDateValidator;
import com.dremio.common.util.DateTimes;
import com.dremio.exec.expr.fn.FunctionErrorContext;
import com.google.common.annotations.VisibleForTesting;

/**
 * Helper method to reduce the complexity of TO_XX and IS_XX date functions
 */
public class DateFunctionsUtils {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DateFunctionsUtils.class);

  public static DateTimeFormatter getISOFormatterForFormatString(final String formatString) {
    return getFormatterInternal(formatString, ISOChronology.getInstanceUTC());
  }

  public static DateTimeFormatter getSQLFormatterForFormatString(final String formatString) {
    return getFormatterInternal(formatString, DayOfWeekFromSundayChronology.getISOInstanceInUTC());
  }

  private static DateTimeFormatter getFormatterInternal(final String formatString, final Chronology chronology) {
    final String jodaString;
    try {
      jodaString = JodaDateValidator.toJodaFormat(formatString);
    } catch (ParseException e) {
      throw UserException.functionError(e)
          .message("Failure parsing the formatting string at column %d of: %s", e.getErrorOffset(), formatString)
          .addContext("Details", e.getMessage())
          .addContext("Format String", formatString)
          .addContext("Error Offset", e.getErrorOffset())
          .build(logger);
    }

    try {
      return DateTimeFormat.forPattern(jodaString)
          .withChronology(chronology);
    } catch (IllegalArgumentException ex) {
      throw UserException.functionError(ex)
          .message("Invalid formatting string")
          .addContext("Details", ex.getMessage())
          .addContext("Format String", formatString)
          .build(logger);
    }
  }

  public static DateTimeFormatter getSQLFormatterForFormatString(final String formatString, FunctionErrorContext errCtx) {
    String jodaString;
    try {
      jodaString = JodaDateValidator.toJodaFormat(formatString);
    } catch (ParseException e) {
      throw errCtx.error()
        .message("Failure parsing the formatting string at column %d of: %s", e.getErrorOffset(), formatString)
        .addContext("Details", e.getMessage())
        .addContext("Format String", formatString)
        .addContext("Error Offset", e.getErrorOffset())
        .build();
    }

    try {
      return DateTimeFormat.forPattern(jodaString)
          .withChronology(DayOfWeekFromSundayChronology.getISOInstanceInUTC());
    } catch (IllegalArgumentException ex) {
      throw errCtx.error()
        .message("Invalid formatting string")
        .addContext("Details", ex.getMessage())
        .addContext("Format String", formatString)
        .build();
    }
  }

  public static long formatDateMilli(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    return formatDate(input, format, errCtx);
  }

  public static long formatDateMilli(String input, DateTimeFormatter format) {
    return formatDate(input, format);
  }

  public static long formatDate(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    try {
      return formatDate(input, format);
    } catch (IllegalArgumentException ex) {
      throw errCtx.error()
      .message("Input text cannot be formatted to date")
      .addContext("Details", ex.getMessage())
      .addContext("Input text", input)
      .build();
    }
  }

  public static long formatDate(String input, DateTimeFormatter format) {
      final DateTime dateTime = format.parseDateTime(input);
      // Subtract out the time part in DateTime
      return DateTimes.toMillis(dateTime) - dateTime.millisOfDay().get();
  }

  public static long formatTimeStampMilli(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    return formatTimeStamp(input, format, errCtx);
  }

  public static long formatTimeStampMilli(String input, DateTimeFormatter format) {
    return formatTimeStamp(input, format);
  }

  public static long formatTimeStamp(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    try {
      return formatTimeStamp(input, format);
    } catch (IllegalArgumentException ex) {
      throw errCtx.error()
        .message("Input text cannot be formatted to date")
        .addContext("Details", ex.getMessage())
        .addContext("Input text", input)
        .build();
    }
  }

  @VisibleForTesting
  static long formatTimeStamp(String input, DateTimeFormatter format) {
    return DateTimes.toMillis(format.parseDateTime(input));
  }

  public static int formatTimeMilli(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    return formatTime(input, format, errCtx);
  }

  public static int formatTimeMilli(String input, DateTimeFormatter format) {
    return formatTime(input, format);
  }

  public static int formatTime(String input, DateTimeFormatter format, FunctionErrorContext errCtx) {
    try {
      return formatTime(input, format);
    } catch (IllegalArgumentException ex) {
      throw errCtx.error()
        .message("Input text cannot be formatted to date")
        .addContext("Details", ex.getMessage())
        .addContext("Input text", input)
        .build();
    }
  }

  public static int formatTime(String input, DateTimeFormatter format) {
    return DateTimes.toMillisOfDay(format.parseDateTime(input));
  }
}
