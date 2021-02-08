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

package com.dremio.dac.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * DateUtils is responsible for provide useful methods for date conversion tasks.
 */
public class DateUtils {

  /**
   * Retrieves the date value of last month's start in milliseconds.
   *
   * @return the date value of last month's start in milliseconds
   */
  public static long getStartOfLastMonth() {
    LocalDate now = LocalDate.now();
    LocalDate targetDate = now.minusDays(now.getDayOfMonth() - 1).minusMonths(1);
    Instant instant = targetDate.atStartOfDay().toInstant(ZoneOffset.UTC);
    return instant.toEpochMilli();
  }

  /**
   *
   * Gets the date of the week's start based on the current weekday.
   * <p>
   * Sunday represents the start of the week.
   *
   * @param dateWithinWeek the current weekday's date
   * @return the date of the week's start based on the current weekday
   */
  public static LocalDate getLastSundayDate(final LocalDate dateWithinWeek) {
    int dayOfWeek = dateWithinWeek.getDayOfWeek().getValue();
    dayOfWeek = (dayOfWeek == 7) ? 0 : dayOfWeek;
    return dateWithinWeek.minusDays(dayOfWeek);
  }

  /**
   * Return the starting date of the month
   *
   * @param dateWithinMonth
   * @return
   */
  public static LocalDate getMonthStartDate(final LocalDate dateWithinMonth) {
    int dayOfMonth = dateWithinMonth.getDayOfMonth();
    return dateWithinMonth.minusDays((dayOfMonth - 1)); // minus one to adjust today
  }

  /**
   * Returns the UTC converted LocalDate from the epoch milliseconds
   *
   * @param epochMillis
   * @return
   */
  public static LocalDate fromEpochMillis(final long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC).toLocalDate();
  }
}
