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

package com.dremio.dac.api;


import static com.dremio.dac.util.DateUtils.getLastSundayDate;
import static com.dremio.dac.util.DateUtils.getMonthStartDate;
import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dremio.common.util.DremioVersionInfo;

/**
 * Tests for {@link UserStats}
 */
public class TestUserStats {

  @Test
  public void testGetEdition() {
    UserStats.Builder stats = new UserStats.Builder();
    stats.setEdition("oss");
    String expected = "dremio-oss-" + DremioVersionInfo.getVersion();
    assertEquals(expected, stats.build().getEdition());
  }

  @Test
  public void testGetStatsByDate() {
    UserStats.Builder statsBuilder = new UserStats.Builder();
    LocalDate now = LocalDate.now();

    // Should log only last 10 days. This should be ignored
    statsBuilder.addUserStat(toEpochMillis(now.minusDays(11)), "UI", "testuser1");

    // Same user and same job type should be counted as one
    statsBuilder.addUserStat(toEpochMillis(now.minusDays(9)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusDays(9)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusDays(9)), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusDays(9)), "ODBC", "testuser2");

    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser2");

    UserStats stats = statsBuilder.build();
    List<Map<String, Object>> statsByDate = stats.getUserStatsByDate();
    assertEquals(2, statsByDate.size());

    Map<String, Object> firstDateEntry = fetchDateEntry("date", now.minusDays(9).toString(), statsByDate);
    assertEquals(1, firstDateEntry.get("UI"));
    assertEquals(2, firstDateEntry.get("ODBC"));
    assertEquals(2, firstDateEntry.get("total"));

    Map<String, Object> secondDateEntry = fetchDateEntry("date", now.toString(), statsByDate);
    assertEquals(1, secondDateEntry.get("UI"));
    assertEquals(2, secondDateEntry.get("ODBC"));
    assertEquals(2, secondDateEntry.get("total"));
  }

  @Test
  public void testGetStatsByWeek() {
    UserStats.Builder statsBuilder = new UserStats.Builder();
    LocalDate now = LocalDate.now();

    // Should log only last 2 weeks. This should be ignored
    statsBuilder.addUserStat(toEpochMillis(now.minusWeeks(3)), "UI", "testuser1");

    // Same user and same job type should be counted as one
    statsBuilder.addUserStat(toEpochMillis(now.minusWeeks(1)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusWeeks(1)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusWeeks(1)), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusWeeks(1)), "ODBC", "testuser2");

    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser2");

    UserStats stats = statsBuilder.build();
    List<Map<String, Object>> statsByDate = stats.getUserStatsByWeek();
    assertEquals(2, statsByDate.size());

    Map<String, Object> firstDateEntry = fetchDateEntry("week", getLastSundayDate(now.minusWeeks(1)).toString(), statsByDate);
    assertEquals(1, firstDateEntry.get("UI"));
    assertEquals(2, firstDateEntry.get("ODBC"));
    assertEquals(2, firstDateEntry.get("total"));

    Map<String, Object> secondDateEntry = fetchDateEntry("week", getLastSundayDate(now).toString(), statsByDate);
    assertEquals(1, secondDateEntry.get("UI"));
    assertEquals(2, secondDateEntry.get("ODBC"));
    assertEquals(2, secondDateEntry.get("total"));
  }

  @Test
  public void testGetStatsByMonth() {
    UserStats.Builder statsBuilder = new UserStats.Builder();
    LocalDate now = LocalDate.now();

    // Same user and same job type should be counted as one
    statsBuilder.addUserStat(toEpochMillis(now.minusMonths(1)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusMonths(1)), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusMonths(1)), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now.minusMonths(1)), "ODBC", "testuser2");

    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "UI", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser1");
    statsBuilder.addUserStat(toEpochMillis(now), "ODBC", "testuser2");

    List<Map<String, Object>> statsByDate = statsBuilder.build().getUserStatsByMonth();
    assertEquals(2, statsByDate.size());

    Map<String, Object> firstDateEntry = fetchDateEntry("month", getMonthStartDate(now.minusMonths(1)).toString(), statsByDate);
    assertEquals(1, firstDateEntry.get("UI"));
    assertEquals(2, firstDateEntry.get("ODBC"));
    assertEquals(2, firstDateEntry.get("total"));

    Map<String, Object> secondDateEntry = fetchDateEntry("month", getMonthStartDate(now).toString(), statsByDate);
    assertEquals(1, secondDateEntry.get("UI"));
    assertEquals(2, secondDateEntry.get("ODBC"));
    assertEquals(2, secondDateEntry.get("total"));
  }

  public static Map<String, Object> fetchDateEntry(String entryKey, String entryVal, List<Map<String, Object>> stats) {
    for (Map<String, Object> dateStats : stats) {
      outerLoop:
      for (Map.Entry<String, Object> stat : dateStats.entrySet()) {
        if (!stat.getKey().equalsIgnoreCase(entryKey)) {
          continue;
        }

        if (stat.getValue().equals(entryVal)) {
          return dateStats;
        } else {
          continue outerLoop;
        }
      }
    }
    throw new IllegalStateException("Date entry not found");
  }

  public static long toEpochMillis(LocalDate targetDate) {
    Instant instant = targetDate.atStartOfDay().toInstant(ZoneOffset.UTC);
    return instant.toEpochMilli();
  }
}
