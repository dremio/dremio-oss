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
package com.dremio.dac.model.job;

import static com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME;
import static com.dremio.dac.server.test.SampleDataPopulator.TEST_USER_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dremio.dac.daemon.TestSpacesStoragePlugin;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.DatasetUI;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.ParentDatasetInfo;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.job.proto.SessionId;
import com.dremio.service.jobs.HybridJobsService;
import com.dremio.service.jobs.Job;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.DatasetType;
import com.dremio.service.users.SimpleUser;
import com.dremio.service.users.UserService;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestJobsListingUI extends BaseTestServer {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  private HybridJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) l(JobsService.class);
    TestSpacesStoragePlugin.setup();
    getPopulator().populateTestUsers();
  }

  @Test
  public void testJobsListingUI() throws Exception {
    String sql = "select * from testA.dsA1";
    // job1
    final JobId jobId1 =
        localSubmitJobAndWaitUntilCompletion(sql, DEFAULT_USER_NAME, QueryType.UI_RUN);
    final Job job1 =
        createJob(
            jobId1.getId(),
            Arrays.asList("testA", "dsA1"),
            "v1",
            DEFAULT_USER_NAME,
            "testA",
            JobState.COMPLETED,
            sql,
            100L,
            110L,
            QueryType.UI_RUN);
    ParentDatasetInfo parentDatasetInfo = new ParentDatasetInfo();
    parentDatasetInfo.setDatasetPathList(Arrays.asList("testA", "dsA1"));
    parentDatasetInfo.setType(DatasetType.valueOf(2));
    job1.getJobAttempt().getInfo().setParentsList(Collections.singletonList(parentDatasetInfo));

    com.dremio.service.job.proto.JobDetails jobDetails = JobDetails.getDefaultInstance();
    jobDetails.setWaitInClient(1L);
    job1.getJobAttempt().setDetails(jobDetails);

    // job2
    final JobId jobId2 = localSubmitJobAndWaitUntilCompletion(sql, TEST_USER_NAME, QueryType.JDBC);
    final Job job2 =
        createJob(
            jobId2.getId(),
            Arrays.asList("testA", "dsA1"),
            "v1",
            TEST_USER_NAME,
            "testA",
            JobState.COMPLETED,
            sql,
            100L,
            110L,
            QueryType.JDBC);
    job2.getJobAttempt().getInfo().setParentsList(Collections.singletonList(parentDatasetInfo));
    job2.getJobAttempt().setDetails(jobDetails);

    // add job in jobSummary list
    List<JobSummary> jobs = new ArrayList<>();
    jobs.add(
        jobsService.getJobSummary(
            JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId1)).build()));
    jobs.add(
        jobsService.getJobSummary(
            JobSummaryRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId2)).build()));
    JobsListingUI jobsListingUI = new JobsListingUI(jobs, jobsService, null);

    assertEquals(jobId1.getId(), jobsListingUI.getJobs().get(0).getId());
    assertEquals(3, jobsListingUI.getJobs().get(0).getState().getNumber());
    // Query type
    assertEquals(QueryType.UI_RUN, jobsListingUI.getJobs().get(0).getQueryType());
    assertEquals(QueryType.JDBC, jobsListingUI.getJobs().get(1).getQueryType());
    // user
    assertEquals(DEFAULT_USER_NAME, jobsListingUI.getJobs().get(0).getUser());
    assertEquals(TEST_USER_NAME, jobsListingUI.getJobs().get(1).getUser());

    assertEquals(sql, jobsListingUI.getJobs().get(0).getQueryText());
    assertTrue(jobsListingUI.getJobs().get(0).isComplete());
    assertNotNull(jobsListingUI.getJobs().get(0).getEnqueuedTime());
    assertFalse(jobsListingUI.getJobs().get(0).isAccelerated());
    assertNotNull(jobsListingUI.getJobs().get(0).getPlannerEstimatedCost().toString());
    assertEquals("", jobsListingUI.getJobs().get(0).getEngine());
    assertEquals("", jobsListingUI.getJobs().get(0).getSubEngine());
    assertEquals("SMALL", jobsListingUI.getJobs().get(0).getWlmQueue());
    // Job Status
    assertEquals(JobState.COMPLETED, jobsListingUI.getJobs().get(0).getState());
    assertEquals(JobState.COMPLETED, jobsListingUI.getJobs().get(1).getState());

    assertTrue(jobsListingUI.getJobs().get(0).isComplete());
    // Duration Details
    assertNotNull(jobsListingUI.getJobs().get(0).getDurationDetails());
    assertNotNull(jobsListingUI.getJobs().get(1).getDurationDetails());
    // sql Statment
    assertEquals("select * from testA.dsA1", jobsListingUI.getJobs().get(0).getQueryText());
    assertEquals("1000", jobsListingUI.getJobs().get(0).getRowsScanned().toString());
    assertEquals("1000", jobsListingUI.getJobs().get(0).getOutputRecords().toString());
    assertEquals("15 KB / 1000 Records", jobsListingUI.getJobs().get(0).getInput());
    assertEquals("15 KB / 1000 Records", jobsListingUI.getJobs().get(0).getOutput());
    assertTrue(jobsListingUI.getJobs().get(0).getWaitInClient() >= 0);
    assertFalse(jobsListingUI.getJobs().get(0).isAccelerated());
    // Queried Datasets
    assertEquals(1, jobsListingUI.getJobs().get(0).getQueriedDatasets().size());
    assertEquals(
        "VIRTUAL_DATASET",
        jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetType());
    assertEquals(
        "dsA1", jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetName());
    assertEquals(
        "testA.dsA1", jobsListingUI.getJobs().get(0).getQueriedDatasets().get(0).getDatasetPath());
  }

  @Test
  public void testJobsListingUIFilter() throws Exception {
    String sql = "select * from testA.dsA1";

    createUsers(Arrays.asList("A", "B", "C", "D", "E"));

    final JobId jobId1 = localSubmitJobAndWaitUntilCompletion(sql, "A", QueryType.UI_RUN);
    final JobId jobId2 = localSubmitJobAndWaitUntilCompletion(sql, "B", QueryType.UI_RUN);
    final JobId jobId3 = localSubmitJobAndWaitUntilCompletion(sql, "C", QueryType.REST);
    final JobId jobId4 = localSubmitJobAndWaitUntilCompletion(sql, "D", QueryType.JDBC);
    final JobId jobId5 = localSubmitJobAndWaitUntilCompletion(sql, "E", QueryType.ODBC);
    // test data creation
    Job jobA1 =
        createJob(
            jobId1.getId(),
            Arrays.asList("space1", "ds1"),
            "v1",
            "A",
            "space1",
            JobState.COMPLETED,
            sql,
            100L,
            110L,
            QueryType.UI_RUN);
    Job jobA2 =
        createJob(
            jobId2.getId(),
            Arrays.asList("space1", "ds1"),
            "v1",
            "B",
            "space1",
            JobState.COMPLETED,
            sql,
            250L,
            260L,
            QueryType.UI_RUN);
    Job jobA3 =
        createJob(
            jobId3.getId(),
            Arrays.asList("space1", "ds1"),
            "v2",
            "C",
            "space1",
            JobState.COMPLETED,
            sql,
            500L,
            400L,
            QueryType.REST);
    Job jobA4 =
        createJob(
            jobId4.getId(),
            Arrays.asList("space1", "ds1"),
            "v3",
            "D",
            "space1",
            JobState.COMPLETED,
            sql,
            200L,
            null,
            QueryType.JDBC);
    Job jobA5 =
        createJob(
            jobId5.getId(),
            Arrays.asList("space1", "ds1"),
            "v2",
            "E",
            "space1",
            JobState.COMPLETED,
            sql,
            400L,
            301L,
            QueryType.UI_INTERNAL_PREVIEW);

    // filter on query type
    List<JobSummary> jobsFilter =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setFilterString("qt==UI")
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    assertEquals(2, jobsFilter.size());

    // filter on job id
    jobsFilter =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setFilterString("*=contains=" + jobId1.getId())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    JobsListingUI jobsListingUI = new JobsListingUI(jobsFilter, jobsService, null);
    assertEquals(jobId1.getId(), jobsListingUI.getJobs().get(0).getId());
    assertEquals(1, jobsFilter.size());

    // search by job state
    jobsFilter =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setFilterString("jst==COMPLETED")
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobsFilter, jobsService, null);
    assertEquals(jobId2.getId(), jobsListingUI.getJobs().get(4).getId());
    assertEquals(8, jobsFilter.size());

    // filter by user
    jobsFilter =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setFilterString("usr==B")
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobsFilter, jobsService, null);
    assertEquals(jobId2.getId(), jobsListingUI.getJobs().get(0).getId());
    assertEquals(1, jobsFilter.size());
  }

  private void createUsers(List<String> asList) {
    UserService userService = getUserService();
    asList.forEach(
        userName -> {
          try {
            userService.createUser(
                SimpleUser.newBuilder().setUserName(userName).build(), DEFAULT_PASSWORD);
          } catch (Exception e) {
          }
        });
  }

  @Test
  public void testJobsListingUISort() throws Exception {
    String sql = "select * from testA.dsA1";

    createUsers(Arrays.asList("A", "B", "C", "D", "E"));

    final JobId jobId1 = localSubmitJobAndWaitUntilCompletion(sql, "A", QueryType.UI_RUN);
    final JobId jobId2 = localSubmitJobAndWaitUntilCompletion(sql, "B", QueryType.UI_RUN);
    final JobId jobId3 = localSubmitJobAndWaitUntilCompletion(sql, "C", QueryType.REST);
    final JobId jobId4 = localSubmitJobAndWaitUntilCompletion(sql, "D", QueryType.JDBC);
    final JobId jobId5 =
        localSubmitJobAndWaitUntilCompletion(sql, "E", QueryType.UI_INTERNAL_PREVIEW);
    // test data creation
    Job jobA1 =
        createJob(
            jobId1.getId(),
            Arrays.asList("space1", "ds1"),
            "v1",
            "A",
            "space1",
            JobState.COMPLETED,
            sql,
            100L,
            110L,
            QueryType.UI_RUN);
    Job jobA2 =
        createJob(
            jobId2.getId(),
            Arrays.asList("space1", "ds1"),
            "v1",
            "B",
            "space1",
            JobState.COMPLETED,
            sql,
            250L,
            260L,
            QueryType.UI_RUN);
    Job jobA3 =
        createJob(
            jobId3.getId(),
            Arrays.asList("space1", "ds1"),
            "v2",
            "C",
            "space1",
            JobState.COMPLETED,
            sql,
            500L,
            400L,
            QueryType.REST);
    Job jobA4 =
        createJob(
            jobId4.getId(),
            Arrays.asList("space1", "ds1"),
            "v3",
            "D",
            "space1",
            JobState.COMPLETED,
            sql,
            200L,
            null,
            QueryType.JDBC);
    Job jobA5 =
        createJob(
            jobId5.getId(),
            Arrays.asList("space1", "ds1"),
            "v2",
            "E",
            "space1",
            JobState.COMPLETED,
            sql,
            600L,
            301L,
            QueryType.UI_INTERNAL_PREVIEW);

    // sort on start time
    List<JobSummary> jobSortSt =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setSortColumn("st")
                    .setSortOrder(ResultOrder.ASCENDING.toSortOrder())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    JobsListingUI jobsListingUI = new JobsListingUI(jobSortSt, jobsService, null);
    assertEquals(jobId1.getId(), jobsListingUI.getJobs().get(0).getId());

    jobSortSt =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setSortColumn("st")
                    .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobSortSt, jobsService, null);
    assertEquals(jobId5.getId(), jobsListingUI.getJobs().get(0).getId());

    // sort on jobId
    List<String> jobListSorted =
        Lists.newArrayList(
            jobId1.getId(), jobId2.getId(), jobId3.getId(), jobId4.getId(), jobId5.getId());
    jobListSorted = jobListSorted.stream().sorted().collect(Collectors.toList());

    jobSortSt =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setSortColumn("job")
                    .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobSortSt, jobsService, null);
    assertEquals(jobListSorted.get(0), jobsListingUI.getJobs().get(4).getId());

    jobSortSt =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setSortColumn("job")
                    .setSortOrder(ResultOrder.ASCENDING.toSortOrder())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobSortSt, jobsService, null);
    assertEquals(jobListSorted.get(0), jobsListingUI.getJobs().get(0).getId());

    // sort on user
    jobSortSt =
        ImmutableList.copyOf(
            jobsService.searchJobs(
                SearchJobsRequest.newBuilder()
                    .setSortColumn("usr")
                    .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
                    .setUserName(DEFAULT_USERNAME)
                    .build()));
    jobsListingUI = new JobsListingUI(jobSortSt, jobsService, null);
    assertEquals(jobId5.getId(), jobsListingUI.getJobs().get(0).getId());
  }

  private JobId localSubmitJobAndWaitUntilCompletion(String sql, String user, QueryType queryType) {
    DatasetUI dsGet = getDataset(new DatasetPath("testA.dsA1"));
    DatasetVersion v2 = DatasetVersion.newVersion();

    return submitJobAndWaitUntilCompletion(
        JobRequest.newBuilder()
            .setSqlQuery(new SqlQuery(sql, user))
            .setQueryType(queryType)
            .setDatasetPath(getDatasetPath(dsGet).toNamespaceKey())
            .setDatasetVersion(v2)
            .build());
  }

  private Job createJob(
      final String id,
      final List<String> datasetPath,
      final String version,
      final String user,
      final String space,
      final JobState state,
      final String sql,
      final Long start,
      final Long end,
      QueryType queryType) {
    final JobId jobId = new JobId(id);
    final JobInfo jobInfo =
        new JobInfo(jobId, sql, version, QueryType.UI_RUN)
            .setClient("client")
            .setDatasetPathList(datasetPath)
            .setUser(user)
            .setSpace(space)
            .setStartTime(start)
            .setFinishTime(end)
            .setQueryType(queryType)
            .setResourceSchedulingInfo(
                new ResourceSchedulingInfo().setQueueName("SMALL").setRuleName("ruleSmall"));

    final JobAttempt jobAttempt = new JobAttempt().setState(state).setInfo(jobInfo);

    return new Job(jobId, jobAttempt, new SessionId());
  }
}
