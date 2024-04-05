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

import static com.dremio.dac.api.TestUserStats.fetchDateEntry;
import static com.dremio.dac.api.TestUserStats.toEpochMillis;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.dac.util.DateUtils;
import com.dremio.edition.EditionProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.options.OptionManager;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.jobs.JobsService;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/** Tests for {@link UserStatsResource} */
public class TestUserStatsResource {
  @Test
  public void testActiveUserStats() {
    EditionProvider editionProvider = mock(EditionProvider.class);
    when(editionProvider.getEdition()).thenReturn("oss-test");

    OptionManager optionManager = mock(OptionManager.class);
    when(optionManager.getOption(ExecConstants.ENABLE_DEPRECATED_JOBS_USER_STATS_API))
        .thenReturn(true);

    LocalDate now = LocalDate.now();

    List<JobSummary> testJobResults = new ArrayList<>();
    testJobResults.add(newJob("testuser1", QueryType.ODBC, now));
    testJobResults.add(newJob("testuser1", QueryType.ODBC, now)); // repeat user should get ignored
    testJobResults.add(newJob("testuser1", QueryType.UI_PREVIEW, now));
    testJobResults.add(newJob("testuser2", QueryType.ODBC, now));

    // at start of the week.. should get ignored in the weekly stats
    testJobResults.add(newJob("testuser1", QueryType.ODBC, DateUtils.getLastSundayDate(now)));
    testJobResults.add(
        newJob(
            "testuser1",
            QueryType.ODBC,
            DateUtils.getLastSundayDate(now))); // repeat user should get ignored
    testJobResults.add(newJob("testuser1", QueryType.UI_PREVIEW, DateUtils.getLastSundayDate(now)));
    testJobResults.add(newJob("testuser2", QueryType.ODBC, DateUtils.getLastSundayDate(now)));

    // at start of the month.. should get ignored in monthly stats
    testJobResults.add(newJob("testuser1", QueryType.ODBC, DateUtils.getMonthStartDate(now)));
    testJobResults.add(
        newJob(
            "testuser1",
            QueryType.ODBC,
            DateUtils.getMonthStartDate(now))); // repeat user should get ignored
    testJobResults.add(newJob("testuser1", QueryType.UI_PREVIEW, DateUtils.getMonthStartDate(now)));
    testJobResults.add(newJob("testuser2", QueryType.ODBC, DateUtils.getMonthStartDate(now)));

    // at start of the last month.. should get included in monthly stats
    testJobResults.add(
        newJob("testuser1", QueryType.ODBC, DateUtils.getMonthStartDate(now.minusMonths(1))));
    testJobResults.add(
        newJob(
            "testuser1",
            QueryType.ODBC,
            DateUtils.getMonthStartDate(now.minusMonths(1)))); // repeat user should get ignored
    testJobResults.add(
        newJob("testuser1", QueryType.UI_PREVIEW, DateUtils.getMonthStartDate(now.minusMonths(1))));
    testJobResults.add(
        newJob("testuser2", QueryType.ODBC, DateUtils.getMonthStartDate(now.minusMonths(1))));

    JobsService jobsService = mock(JobsService.class);
    when(jobsService.searchJobs(any(SearchJobsRequest.class))).thenReturn(testJobResults);

    UserStatsResource resource = new UserStatsResource(jobsService, editionProvider, optionManager);
    UserStats stats = resource.getActiveUserStats(0, 0, "false");

    List<Map<String, Object>> statsByDate = stats.getUserStatsByDate();
    Map<String, Object> firstDateEntry = fetchDateEntry("date", now.toString(), statsByDate);
    assertEquals(1, firstDateEntry.get("UI_PREVIEW"));
    assertEquals(2, firstDateEntry.get("ODBC"));
    assertEquals(2, firstDateEntry.get("total"));

    Map<String, Object> secondDateEntry =
        fetchDateEntry("date", DateUtils.getLastSundayDate(now).toString(), statsByDate);
    assertEquals(1, secondDateEntry.get("UI_PREVIEW"));
    assertEquals(2, secondDateEntry.get("ODBC"));
    assertEquals(2, secondDateEntry.get("total"));

    Map<String, Object> weekDateEntry =
        fetchDateEntry(
            "week", DateUtils.getLastSundayDate(now).toString(), stats.getUserStatsByWeek());
    assertEquals(1, weekDateEntry.get("UI_PREVIEW"));
    assertEquals(2, weekDateEntry.get("ODBC"));
    assertEquals(2, weekDateEntry.get("total"));

    Map<String, Object> monthDateEntry =
        fetchDateEntry(
            "month", DateUtils.getMonthStartDate(now).toString(), stats.getUserStatsByMonth());
    assertEquals(1, monthDateEntry.get("UI_PREVIEW"));
    assertEquals(2, monthDateEntry.get("ODBC"));
    assertEquals(2, monthDateEntry.get("total"));

    Map<String, Object> lastMonthEntry =
        fetchDateEntry(
            "month",
            DateUtils.getMonthStartDate(now.minusMonths(1)).toString(),
            stats.getUserStatsByMonth());
    assertEquals(1, lastMonthEntry.get("UI_PREVIEW"));
    assertEquals(2, lastMonthEntry.get("ODBC"));
    assertEquals(2, lastMonthEntry.get("total"));
  }

  private JobSummary newJob(String user, QueryType queryType, LocalDate startDate) {
    return JobSummary.newBuilder()
        .setUser(user)
        .setQueryType(queryType)
        .setStartTime(toEpochMillis(startDate))
        .build();
  }
}
