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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dremio.common.util.DremioVersionInfo;
import com.dremio.service.job.JobAndUserStats;
import com.dremio.service.job.JobCountByQueryType;
import com.dremio.service.job.UniqueUsersCountByQueryType;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Job and user stats response.
 */
public class JobAndUserStatsResponse {

  private static String DREMIO_EDITION_FORMAT = "dremio-%s-%s";

  private String edition;

  public static class SummarizedStat {
    private final String date;
    private final Long jobCount;
    private final Long uniqueUsersCount;

    public SummarizedStat(String date, Long jobCount, Long uniqueUsersCount) {
      this.date = date;
      this.jobCount = jobCount;
      this.uniqueUsersCount = uniqueUsersCount;
    }

    public String getDate() {
      return date;
    }

    public Long getJobCount() {
      return jobCount;
    }

    public Long getUniqueUsersCount() {
      return uniqueUsersCount;
    }
  }

  public static class JobStat {
    private final String date;
    private final Long total;
    private final Map<String, Long> detailedStats;

    public JobStat(String date, Long total, List<JobCountByQueryType> jobCountByQueryTypes) {
      this.date = date;
      this.total = total;
      this.detailedStats = new HashMap<>();
      for (JobCountByQueryType jobCountByQueryType : jobCountByQueryTypes) {
        detailedStats.put(jobCountByQueryType.getQueryType().toString(), jobCountByQueryType.getJobCount());
      }
    }

    public String getDate() {
      return date;
    }

    public Long getTotal() {
      return total;
    }

    @JsonAnyGetter
    public Map<String, Long> getDetailedStats() {
      return detailedStats;
    }
  }

  public static class UserStat {
    private final String date;
    private final Long total;
    private final Map<String, Long> detailedStats;

    public UserStat(String date, Long total, List<UniqueUsersCountByQueryType> uniqueUsersCountByQueryTypes) {
      this.date = date;
      this.total = total;
      this.detailedStats = new HashMap<>();
      for (UniqueUsersCountByQueryType uniqueUsersCountByQueryType : uniqueUsersCountByQueryTypes) {
        detailedStats.put(uniqueUsersCountByQueryType.getQueryType().toString(), (long) uniqueUsersCountByQueryType.getUniqueUsersList().size());
      }
    }

    public String getDate() {
      return date;
    }

    public Long getTotal() {
      return total;
    }

    @JsonAnyGetter
    public Map<String, Long> getDetailedStats() {
      return detailedStats;
    }
  }

  public static class WeeklyUserStat extends UserStat {
    private final String week;

    public WeeklyUserStat(String date, Long total, List<UniqueUsersCountByQueryType> uniqueUsersCountByQueryTypes) {
      super(date, total, uniqueUsersCountByQueryTypes);
      this.week = date;
    }

    public String getWeek() {
      return week;
    }

    @Override
    @JsonIgnore
    public String getDate() {
      return super.getDate();
    }
  }

  public static class MonthlyUserStat extends UserStat {
    private final String month;

    public MonthlyUserStat(String date, Long total, List<UniqueUsersCountByQueryType> uniqueUsersCountByQueryTypes) {
      super(date, total, uniqueUsersCountByQueryTypes);
      this.month = date;
    }

    public String getMonth() {
      return month;
    }

    @Override
    @JsonIgnore
    public String getDate() {
      return super.getDate();
    }
  }

  private final List<SummarizedStat> stats;

  private final List<JobStat> jobStats;

  private final List<UserStat> userStatsByDate;

  private final List<WeeklyUserStat> userStatsByWeek;

  private final List<MonthlyUserStat> userStatsByMonth;

  public JobAndUserStatsResponse() {
    stats = new ArrayList<>();
    jobStats = new ArrayList<>();
    userStatsByDate = new ArrayList<>();
    userStatsByWeek = new ArrayList<>();
    userStatsByMonth = new ArrayList<>();
  }

  public List<SummarizedStat> getStats() {
    return stats;
  }

  public void setStats(com.dremio.service.job.JobAndUserStats stats) {
    for (com.dremio.service.job.JobAndUserStat stat : stats.getStatsList()) {
      this.stats.add(new SummarizedStat(stat.getDate(), stat.getTotalJobs(), stat.getTotalUniqueUsers()));
    }
  }

  public String getEdition() {
    return edition;
  }

  public void setEdition(String edition) {
    this.edition = edition;
  }

  public List<JobStat> getJobStats() {
    return jobStats;
  }

  public void setJobStats(com.dremio.service.job.JobAndUserStats stats) {
    for (com.dremio.service.job.JobAndUserStat stat : stats.getStatsList()) {
      if (stat.getTotalJobs() == 0) {
        continue;
      }
      jobStats.add(new JobStat(stat.getDate(), stat.getTotalJobs(), stat.getJobCountByQueryTypeList()));
    }
  }

  public List<UserStat> getUserStatsByDate() {
    return userStatsByDate;
  }

  public void setUserStatsByDate(com.dremio.service.job.JobAndUserStats stats) {
    for (com.dremio.service.job.JobAndUserStat stat : stats.getStatsList()) {
      if (stat.getIsWeeklyStat() || stat.getIsMonthlyStat() || stat.getTotalJobs() == 0) {
        continue;
      }
      userStatsByDate.add(new UserStat(stat.getDate(), stat.getTotalUniqueUsers(), stat.getUniqueUsersCountByQueryTypeList()));
    }
  }

  public List<WeeklyUserStat> getUserStatsByWeek() {
    return userStatsByWeek;
  }

  public void setUserStatsByWeek(com.dremio.service.job.JobAndUserStats stats) {
    for (com.dremio.service.job.JobAndUserStat stat : stats.getStatsList()) {
      if (!stat.getIsWeeklyStat()) {
        continue;
      }
      userStatsByWeek.add(new WeeklyUserStat(stat.getDate(), stat.getTotalUniqueUsers(), stat.getUniqueUsersCountByQueryTypeList()));
    }
  }

  public List<MonthlyUserStat> getUserStatsByMonth() {
    return userStatsByMonth;
  }

  public void setUserStatsByMonth(com.dremio.service.job.JobAndUserStats stats) {
    for (com.dremio.service.job.JobAndUserStat stat : stats.getStatsList()) {
      if (!stat.getIsMonthlyStat()) {
        continue;
      }
      userStatsByMonth.add(new MonthlyUserStat(stat.getDate(), stat.getTotalUniqueUsers(), stat.getUniqueUsersCountByQueryTypeList()));
    }
  }

  public static JobAndUserStatsResponse createJobAndUserStatsResource(JobAndUserStats jobAndUserStats, String edition, boolean detailedStats) {
    JobAndUserStatsResponse jobAndUserStatsResponse = new JobAndUserStatsResponse();
    jobAndUserStatsResponse.setEdition(String.format(DREMIO_EDITION_FORMAT, edition, DremioVersionInfo.getVersion()));
    if (!detailedStats) {
      jobAndUserStatsResponse.setStats(jobAndUserStats);
    } else {
      jobAndUserStatsResponse.setJobStats(jobAndUserStats);
      jobAndUserStatsResponse.setUserStatsByDate(jobAndUserStats);
      jobAndUserStatsResponse.setUserStatsByWeek(jobAndUserStats);
      jobAndUserStatsResponse.setUserStatsByMonth(jobAndUserStats);
    }
    return jobAndUserStatsResponse;
  }

}
