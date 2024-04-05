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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.edition.EditionProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.QueryType;
import com.dremio.service.jobs.JobsService;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

/** tests the aggregated stats of jobs per day */
public class TestDailyJobStats {
  private static final String PATH = "/cluster/jobstats";

  @Test
  public void testTotalCounts() {
    // test total count
    final List<JobSummary> jobs = new ArrayList<>();
    // populate for 14 days
    final long epoch = System.currentTimeMillis();
    final long dayEpoch = (24 * 60 * 60 * 1000L);
    for (int i = 0; i < 14; ++i) {
      // every day add 3 jobs
      jobs.add(newJob("dremio", QueryType.UI_RUN, epoch - (i * dayEpoch)));
      jobs.add(newJob("dremio", QueryType.JDBC, epoch - (i * dayEpoch)));
      jobs.add(newJob("dremio", QueryType.ODBC, epoch - (i * dayEpoch)));
    }

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_DEPRECATED_JOBS_USER_STATS_API))
        .thenReturn(true);

    final DailyJobStatsResource dailyJobStatsResource =
        new DailyJobStatsResource(
            mock(JobsService.class), mock(EditionProvider.class), optionManager);
    final DailyJobStatsResource.DailyJobStats results =
        dailyJobStatsResource.aggregateJobResults(jobs);

    final List<Map<String, Object>> stats = results.getJobStats();

    for (Map<String, Object> day : stats) {
      Assert.assertEquals(3L, day.get("total"));
      Assert.assertEquals(1L, day.get("UI_RUN"));
      Assert.assertEquals(1L, day.get("JDBC"));
      Assert.assertEquals(1L, day.get("ODBC"));
    }
  }

  @Test
  public void testWithLargeJobCount() {
    final long jobCount = 1_000_000L;

    final JobsService service = mock(JobsService.class);
    when(service.searchJobs(any())).thenReturn(() -> prepareJobSummaryData(jobCount));

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_DEPRECATED_JOBS_USER_STATS_API))
        .thenReturn(true);

    final DailyJobStatsResource dailyJobStatsResource =
        new DailyJobStatsResource(service, mock(EditionProvider.class), optionManager);
    final DailyJobStatsResource.DailyJobStats results =
        dailyJobStatsResource.getStats(0, 0, "false");

    final List<Map<String, Object>> stats = results.getJobStats();

    for (Map<String, Object> day : stats) {
      Assert.assertEquals(jobCount, day.get("total"));
      Assert.assertEquals(jobCount, day.get("UI_RUN"));
    }
  }

  private JobSummary newJob(String user, QueryType queryType, long startTime) {
    // populate jobsummary, make it a bit heap expensive
    return JobSummary.newBuilder()
        .setUser(user)
        .setQueryType(queryType)
        .setStartTime(startTime)
        .setDatasetVersion("random_data_path")
        .setDescription("A random description to consume some heap memory")
        .setSql("SELECT * FROM SOME_TABLE ORDER BY A, B, C, D , E LIMIT 100 OFFSET 5000")
        .setNumAttempts(1_000_000)
        .setOutputRecords(1_000_000)
        .setFailureInfo("no job failure")
        .setRecordCount(10_000_000)
        .setAccelerated(true)
        .setSpilled(true)
        .setEndTime(startTime + 100000)
        .setJobState(JobState.COMPLETED)
        .build();
  }

  private Iterator<JobSummary> prepareJobSummaryData(final long n) {
    final long epoch = System.currentTimeMillis();
    return Stream.generate(() -> newJob("dremio", QueryType.UI_RUN, epoch)).limit(n).iterator();
  }
}
