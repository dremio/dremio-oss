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
package com.dremio.exec.expr.fn.impl;

import java.text.ParseException;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.expression.fn.JodaDateValidator;
/**
 * Helper method to reduce the complexity of TO_XX and IS_XX date functions
 */
public class DateFunctionsUtils {
  public static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DateFunctionsUtils.class);

  public static DateTimeFormatter getFormatterForFormatString(final String formatString) {
    String jodaString = null;
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
      return DateTimeFormat.forPattern(jodaString).withZoneUTC();
    } catch (IllegalArgumentException ex) {
      throw UserException.functionError(ex)
        .message("Invalid formatting string")
        .addContext("Details", ex.getMessage())
        .addContext("Format String", formatString)
        .build(logger);
    }
  }

  public static long formatDateMilli(String input, DateTimeFormatter format) {
    return formatDate(input, format);
  }

  public static long formatDate(String input, DateTimeFormatter format) {
    try {
      final DateTime dateTime = format.parseDateTime(input);

      // Subtract out the time part in DateTime
      return com.dremio.common.util.DateTimes.toMillis(dateTime) - dateTime.millisOfDay().get();
    } catch (IllegalArgumentException ex) {
      throw UserException.functionError(ex)
      .message("Input text cannot be formatted to date")
      .addContext("Details", ex.getMessage())
      .addContext("Input text", input)
      .build(logger);
    }
  }

  public static long formatTimeStampMilli(String input, DateTimeFormatter format) {
    return formatTimeStamp(input, format);
  }

  public static long formatTimeStamp(String input, DateTimeFormatter format) {
    try {
      return com.dremio.common.util.DateTimes.toMillis(format.parseDateTime(input));
    } catch (IllegalArgumentException ex) {
      throw UserException.functionError(ex)
        .message("Input text cannot be formatted to date")
        .addContext("Details", ex.getMessage())
        .addContext("Input text", input)
        .build(logger);
    }
  }

  public static int formatTimeMilli(String input, DateTimeFormatter format) {
    return formatTime(input, format);
  }

  public static int formatTime(String input, DateTimeFormatter format) {
    try {
      return com.dremio.common.util.DateTimes.toMillisOfDay(format.parseDateTime(input));
    } catch (IllegalArgumentException ex) {
      throw UserException.functionError(ex)
        .message("Input text cannot be formatted to date")
        .addContext("Details", ex.getMessage())
        .addContext("Input text", input)
        .build(logger);
    }
  }
}
