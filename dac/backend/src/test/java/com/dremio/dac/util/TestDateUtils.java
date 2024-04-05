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

import static org.junit.Assert.assertEquals;

import java.time.LocalDate;
import org.junit.Test;

/** Tests for {@link DateUtils} */
public class TestDateUtils {
  @Test
  public void testGetStartOfLastMonth() {
    LocalDate dateLastMonth = LocalDate.now().minusMonths(1);
    long startOfLastMonth = DateUtils.getStartOfLastMonth();

    LocalDate dateStartOfLastMonth = DateUtils.fromEpochMillis(startOfLastMonth);
    assertEquals(dateLastMonth.getMonthValue(), dateStartOfLastMonth.getMonthValue());
    assertEquals(dateLastMonth.getYear(), dateStartOfLastMonth.getYear());
    assertEquals(1, dateStartOfLastMonth.getDayOfMonth());
  }

  @Test
  public void testGetLastSundayDate() {
    LocalDate testDate1 = LocalDate.parse("2020-03-29");
    LocalDate date1LastSunday = DateUtils.getLastSundayDate(testDate1);
    assertEquals(testDate1, date1LastSunday);

    LocalDate testDate2 = LocalDate.parse("2020-03-28");
    LocalDate date2LastSunday = DateUtils.getLastSundayDate(testDate2);
    assertEquals(LocalDate.parse("2020-03-22"), date2LastSunday);
  }

  @Test
  public void testGetMonthStartDate() {
    LocalDate testDate1 = LocalDate.parse("2020-03-01");
    LocalDate monthStartDate = DateUtils.getMonthStartDate(testDate1);
    assertEquals(testDate1, monthStartDate);

    LocalDate testDate2 = LocalDate.parse("2020-03-31");
    LocalDate date2MonthStartDate = DateUtils.getMonthStartDate(testDate2);
    assertEquals(testDate1, date2MonthStartDate);
  }

  @Test
  public void testFromEpochMillis() {
    LocalDate extractedDate = DateUtils.fromEpochMillis(System.currentTimeMillis());
    assertEquals(LocalDate.now(), extractedDate);
  }
}
