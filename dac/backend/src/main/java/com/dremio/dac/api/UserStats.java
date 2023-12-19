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

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.dac.util.DateUtils;


/**
 * User statistics. Used by {@link UserStatsResource}
 */
public class UserStats {
  private static String DREMIO_EDITION_FORMAT = "dremio-%s-%s";

  private String edition;

  // Contains only the counts. Filled up via the count() method.
  private final List<Map<String, Object>> userStatsByDate = new ArrayList<>();
  private final List<Map<String, Object>> userStatsByWeek = new ArrayList<>();
  private final List<Map<String, Object>> userStatsByMonth = new ArrayList<>();

  public String getEdition() {
    return edition;
  }

  public List<Map<String, Object>> getUserStatsByDate() {
    return userStatsByDate;
  }

  public static UserStats createUserStats(String edition,
                                          List<Map<String, Object>> userStatsByDate,
                                          List<Map<String, Object>> userStatsByWeek,
                                          List<Map<String, Object>> userStatsByMonth) {
    UserStats userStats = new UserStats();
    userStats.edition = String.format(DREMIO_EDITION_FORMAT, edition, DremioVersionInfo.getVersion());
    userStats.userStatsByDate.addAll(userStatsByDate);
    userStats.userStatsByWeek.addAll(userStatsByWeek);
    userStats.userStatsByMonth.addAll(userStatsByMonth);
    return userStats;
  }

  public List<Map<String, Object>> getUserStatsByWeek() {
    return userStatsByWeek;
  }

  public List<Map<String, Object>> getUserStatsByMonth() {
    return userStatsByMonth;
  }

  static class Builder {
    // Contains real user names
    private final Map<LocalDate, UserJobTypeStats> activeUsersByDay = new HashMap<>();
    private final Map<LocalDate, UserJobTypeStats> activeUsersByWeek = new HashMap<>();
    private final Map<LocalDate, UserJobTypeStats> activeUsersByMonth = new HashMap<>();

    // Last two weeks. This gets the date of the Saturday
    private final LocalDate weekStatsLimit = LocalDate.now().minusWeeks(2).plusDays((7 - LocalDate.now().getDayOfWeek().getValue()));
    // Last 10 days
    private final LocalDate dailyStatsLimit = LocalDate.now().minusDays(10);

    private UserStats instance = new UserStats();

    public void setEdition(String edition) {
      instance.edition = String.format(DREMIO_EDITION_FORMAT, edition, DremioVersionInfo.getVersion());
    }

    public void addUserStat(final long startTime, final String jobType, final String user) {
      final LocalDate date = DateUtils.fromEpochMillis(startTime);
      if (!date.isBefore(dailyStatsLimit)) {
        addToStats(activeUsersByDay, date, jobType, user);
      }

      if (!date.isBefore(weekStatsLimit)) {
        addToStats(activeUsersByWeek, getLastSundayDate(date), jobType, user);
      }
      addToStats(activeUsersByMonth, getMonthStartDate(date), jobType, user);
    }

    private void performCountForWindow(String timeUnit, Map<LocalDate, UserJobTypeStats> allUsers, List<Map<String, Object>> countContainer) {
      for (Map.Entry<LocalDate, UserJobTypeStats> usersByDay : allUsers.entrySet()) {
        final Map<String, Object> dateStats = new HashMap<>();
        dateStats.put(timeUnit, usersByDay.getKey().toString());
        for (Map.Entry<String, Set<String>> jobTypeStats : usersByDay.getValue().getActiveUsers().entrySet()) {
          dateStats.put(jobTypeStats.getKey(), jobTypeStats.getValue().size());
        }
        countContainer.add(dateStats);
      }
    }

    public UserStats build() {
      performCountForWindow("date", activeUsersByDay, instance.userStatsByDate);
      performCountForWindow("week", activeUsersByWeek, instance.userStatsByWeek);
      performCountForWindow("month", activeUsersByMonth, instance.userStatsByMonth);
      return instance;
    }

    private void addToStats(Map<LocalDate, UserJobTypeStats> stats, LocalDate date, String jobType, String user) {
      stats.putIfAbsent(date, new UserJobTypeStats());
      UserJobTypeStats userJobTypeStats = stats.get(date);
      userJobTypeStats.getActiveUsers().putIfAbsent(jobType, new HashSet<>());
      userJobTypeStats.getActiveUsers().get(jobType).add(user);

      UserJobTypeStats totalStats = stats.get(date);
      totalStats.getActiveUsers().putIfAbsent("total", new HashSet<>());
      totalStats.getActiveUsers().get("total").add(user);
    }

    private final class UserJobTypeStats {
      private final Map<String, Set<String>> activeUsers = new HashMap<>();
      public Map<String, Set<String>> getActiveUsers() {
        return activeUsers;
      }
    }
  }
}
