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

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.dremio.edition.EditionProvider;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.QueryType;
import com.dremio.service.jobs.JobsService;


/**
 * tests the aggregated stats of jobs per day
 */
public class TestDailyJobStats  {
  private static final String PATH = "/cluster/jobstats";

  @Test
  public void testTotalCounts() {
    //test total count
    final List<JobSummary> jobs = new ArrayList<>();
    //populate for 14 days
    final long epoch = System.currentTimeMillis();
    final long dayEpoch = (24 * 60 * 60 * 1000L);
    for (int i = 0; i < 14; ++i) {
      //every day add 3 jobs
      jobs.add(newJob("dremio", QueryType.UI_RUN, epoch - (i * dayEpoch)));
      jobs.add(newJob("dremio", QueryType.JDBC, epoch - (i * dayEpoch)));
      jobs.add(newJob("dremio", QueryType.ODBC, epoch - (i * dayEpoch )));
    }

    final DailyJobStatsResource dailyJobStatsResource = new DailyJobStatsResource(mock(JobsService.class), mock(EditionProvider.class));
    final DailyJobStatsResource.DailyJobStats results = dailyJobStatsResource.aggregateJobResults(jobs);

    final List<Map<String, Object>> stats = results.getJobStats();

    for(Map<String, Object> day : stats) {
      Assert.assertEquals(3L, day.get("total"));
      Assert.assertEquals(1L, day.get("UI_RUN"));
      Assert.assertEquals(1L, day.get("JDBC"));
      Assert.assertEquals(1L, day.get("ODBC"));
    }
  }

  private JobSummary newJob(String user, QueryType queryType, long startTime) {
    return JobSummary.newBuilder().setUser(user).setQueryType(queryType).setStartTime(startTime).build();
  }
}
