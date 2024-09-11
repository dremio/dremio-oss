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
package com.dremio.service.jobs;

import static com.dremio.dac.server.test.SampleDataPopulator.POPULATOR_USER_NAME;
import static com.dremio.dac.server.test.SampleDataPopulator.TEST_USER_NAME;
import static org.junit.Assert.assertEquals;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.JobAndUserStat;
import com.dremio.service.job.JobAndUserStats;
import com.dremio.service.job.JobAndUserStatsRequest;
import com.dremio.service.job.JobCountByQueryType;
import com.dremio.service.job.UniqueUsersCountByQueryType;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.namespace.dataset.DatasetVersion;
import java.time.LocalDate;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

/** Tests for job and user stats. */
public class TestJobAndUserStats extends BaseTestServer {
  private HybridJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) lMaster(JobsService.class);

    populateJobData();
  }

  private void populateJobData() throws Exception {
    BaseTestServer.getPopulator().populateTestUsers();
    String sql = "SELECT 1";
    String[] users = {DEFAULT_USERNAME, TEST_USER_NAME, POPULATOR_USER_NAME};
    QueryType[] queryTypes = {QueryType.UI_PREVIEW, QueryType.UI_RUN, QueryType.REST};

    JobRequest.Builder jobRequestBuilder =
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(sql, null, users[0]))
            .setDatasetPath(DatasetPath.NONE.toNamespaceKey())
            .setDatasetVersion(DatasetVersion.NONE);

    for (int i = 0; i < users.length; i++) {
      for (int j = 0; j <= i; j++) {
        for (int k = 0; k < 10; k++) {
          submitAndWaitUntilSubmitted(
              jobRequestBuilder.setQueryType(queryTypes[j]).setUsername(users[i]).build());
        }
      }
    }
  }

  private void verifyDetailedUniqueUserStats(
      List<UniqueUsersCountByQueryType> uniqueUsersCountByQueryTypes) {
    for (UniqueUsersCountByQueryType uniqueUserStat : uniqueUsersCountByQueryTypes) {
      switch (uniqueUserStat.getQueryType()) {
        case UI_INTERNAL_RUN:
          assertEquals(1, uniqueUserStat.getUniqueUsersCount());
          break;
        case UI_RUN:
          assertEquals(2, uniqueUserStat.getUniqueUsersCount());
          break;
        case UI_PREVIEW:
          assertEquals(3, uniqueUserStat.getUniqueUsersCount());
          break;
        case REST:
          assertEquals(1, uniqueUserStat.getUniqueUsersCount());
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  private void verifyDetailedJobStats(List<JobCountByQueryType> jobCountByQueryTypes) {
    for (JobCountByQueryType jobCountByQueryType : jobCountByQueryTypes) {
      switch (jobCountByQueryType.getQueryType()) {
        case UI_INTERNAL_RUN:
          assertEquals(1, jobCountByQueryType.getJobCount());
          break;
        case UI_RUN:
          assertEquals(20, jobCountByQueryType.getJobCount());
          break;
        case UI_PREVIEW:
          assertEquals(30, jobCountByQueryType.getJobCount());
          break;
        case REST:
          assertEquals(10, jobCountByQueryType.getJobCount());
          break;
        default:
          throw new IllegalStateException();
      }
    }
  }

  @Test
  public void testUIRequest() {
    JobAndUserStats jobAndUserStats =
        jobsService.getJobAndUserStats(
            JobAndUserStatsRequest.newBuilder().setNumDaysBack(7).setDetailedStats(false).build());
    assertEquals(7, jobAndUserStats.getStatsList().size());
    for (JobAndUserStat stat : jobAndUserStats.getStatsList()) {
      if (stat.getDate().equals(LocalDate.now().toString())) {
        assertEquals(61, stat.getTotalJobs());
        assertEquals(4, stat.getTotalUniqueUsers());
      } else {
        assertEquals(0, stat.getTotalJobs());
        assertEquals(0, stat.getTotalUniqueUsers());
      }
    }
  }

  @Test
  public void testDetailedRequest() {
    JobAndUserStats jobAndUserStats =
        jobsService.getJobAndUserStats(
            JobAndUserStatsRequest.newBuilder().setNumDaysBack(30).setDetailedStats(true).build());
    assertEquals(32, jobAndUserStats.getStatsList().size());
    for (JobAndUserStat stat : jobAndUserStats.getStatsList()) {
      if (stat.getIsWeeklyStat()) {
        assertEquals(
            LocalDate.now().minusDays(LocalDate.now().getDayOfWeek().getValue() % 7).toString(),
            stat.getDate());
        assertEquals(stat.getTotalUniqueUsers(), 4);
        verifyDetailedUniqueUserStats(stat.getUniqueUsersCountByQueryTypeList());
      } else if (stat.getIsMonthlyStat()) {
        assertEquals(
            LocalDate.now().minusDays(LocalDate.now().getDayOfMonth() - 1).toString(),
            stat.getDate());
        assertEquals(stat.getTotalUniqueUsers(), 4);
        verifyDetailedUniqueUserStats(stat.getUniqueUsersCountByQueryTypeList());
      } else {
        if (stat.getDate().equals(LocalDate.now().toString())) {
          assertEquals(61, stat.getTotalJobs());
          assertEquals(4, stat.getTotalUniqueUsers());
          verifyDetailedJobStats(stat.getJobCountByQueryTypeList());
          verifyDetailedUniqueUserStats(stat.getUniqueUsersCountByQueryTypeList());
        } else {
          assertEquals(0, stat.getTotalJobs());
          assertEquals(0, stat.getTotalUniqueUsers());
          assertEquals(0, stat.getJobCountByQueryTypeCount());
          assertEquals(0, stat.getUniqueUsersCountByQueryTypeCount());
        }
      }
    }
  }
}
