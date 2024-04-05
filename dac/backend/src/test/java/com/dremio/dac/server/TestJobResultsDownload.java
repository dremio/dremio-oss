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
package com.dremio.dac.server;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.explore.model.DownloadFormat;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.model.job.ResultOrder;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;
import java.util.Collections;
import java.util.UUID;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

/** Download job results tests. */
public class TestJobResultsDownload extends BaseTestServer {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestJobResultsDownload.class);

  @BeforeClass
  public static void setup() throws Exception {
    // this test should be run for a case, when job results are stored as non pdfs environment
    // this is true for multinode environment, which uses local file system.
    Assume.assumeTrue(isMultinode());
    clearAllDataExceptUser();
  }

  private static SimpleUser newUser(String username) {
    return SimpleUser.newBuilder()
        .setUserName(username)
        .setEmail("test2222@dremio.test")
        .setFirstName("test")
        .setLastName("test")
        .build();
  }

  @Test
  public void downloadFromJob() throws Exception {
    final String user1name = UUID.randomUUID().toString();
    final String password = "password1345";

    l(UserService.class).createUser(newUser(user1name), password);
    login(user1name, password);

    final InitialPreviewResponse response =
        createDatasetFromSQL("select * from (values(1, 2))", Collections.emptyList());
    final String jobId = response.getJobId().getId();
    logger.debug("Job is submitted. Job is: {}", jobId);
    final JobsService jobsService = l(JobsService.class);

    // download json for a submitted job
    expectSuccess(
        getBuilder(
                getAPIv2()
                    .path("job")
                    .path(jobId)
                    .path("download")
                    .queryParam("downloadFormat", DownloadFormat.JSON))
            .buildGet());

    // search for last submitted job by the user.
    Iterable<JobSummary> jobs =
        jobsService.searchJobs(
            SearchJobsRequest.newBuilder()
                .setUserName(user1name)
                .setLimit(1)
                .setSortColumn("st") // start time
                .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
                .build());

    // there must be a job. Take the first one
    final JobSummary jobSummary = jobs.iterator().next();

    assertEquals(
        "Last submitted job by a user should be a UI export job",
        QueryType.UI_EXPORT,
        jobSummary.getQueryType());
    logger.debug("Last job query is: {}", jobSummary.getSql());
    assertTrue(
        "query must contains a query of form sys.job_results.\"{job id}\"",
        jobSummary.getSql().contains(String.format("sys.job_results.\"%s\"", jobId)));
  }
}
