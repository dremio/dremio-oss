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
package com.dremio.dac.support;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

/**
 * DateTimeUtils implements utils for date time compare used in support bundle.
 */
public class DateTimeUtils {

  public static final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
  public static final Pattern DATE_PATTERN = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");

  /**
   * Parses a given string to Date object.
   *
   * @param str the string to be parsed
   * @return    a Date object from a string
   * @throws ParseException If a fail occur during to parse the string to Date format
   */
  public static Date getDateFromString(String str) throws ParseException {

    Matcher matcher = DATE_PATTERN.matcher(str);
    if (matcher.find()) {
      return dateFormat.parse(matcher.group(1));
    }
    throw new ParseException("Date yyyy-MM-dd not found in " + str, 0);
  }


  /**
   * Checks if a given timestamp is within today in zone.
   *
   * @param timestamp the timestamp to be checked
   * @param zone      the zone to be compare
   * @return          a flag which indicates if the timestamp is within today in zone
   */
  public static boolean isToday(long timestamp, DateTimeZone zone) {
    if (timestamp == 0) { // 0 implies query is running now
      return true;
    }
    LocalDate today = new DateTime(zone).toLocalDate();
    return today.equals(new DateTime(timestamp, zone).toLocalDate());
  }

  /**
   * Checks if a given Date is within an interval of two Dates.
   *
   * @param str        the string that container date info in format of "yyyy-mm-dd"
   * @param leftBound  the initial date to validate
   * @param rightBound the final date to validate
   * @return           a flag which indicates if the Date is within an interval of two Dates
   */
  public static boolean isBetweenDay(String str, long leftBound, long rightBound, DateTimeZone zone) {
    Date target;
    try {
      target = getDateFromString(str);
    } catch (ParseException e) {
      return false;
    }
    LocalDate targetDate = new DateTime(target.getTime(), zone).toLocalDate();
    LocalDate leftBoundDate = new DateTime(leftBound, zone).toLocalDate();

    if (leftBound > rightBound) { // only compare with leftBound
      return !targetDate.isBefore(leftBoundDate); // today >= leftBound
    }

    LocalDate rightBoundDate = new DateTime(rightBound, zone).toLocalDate();

    return !targetDate.isBefore(leftBoundDate) && !targetDate.isAfter(rightBoundDate); // today >= leftBound and today <= rightBound
  }

}
