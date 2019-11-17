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

import static com.dremio.dac.proto.model.dataset.OrderDirection.ASC;
import static com.dremio.service.namespace.dataset.DatasetVersion.newVersion;
import static java.util.Arrays.asList;
import static javax.ws.rs.client.Entity.entity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialTransformAndRunResponse;
import com.dremio.dac.model.job.AttemptDetailsUI;
import com.dremio.dac.model.job.AttemptsUIHelper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobFailureInfo;
import com.dremio.dac.model.job.JobFailureType;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.resource.NotificationResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.socket.TestWebSocket;
import com.dremio.datastore.KVStore;
import com.dremio.datastore.KVStoreProvider;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.work.AttemptId;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;

/**
 * Tests for job service.
 */
public class TestJobService extends BaseTestServer {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  private LocalJobsService jobsService;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (LocalJobsService) l(JobsService.class);
  }

  @Test
  public void testCancel() throws Exception {
    final InitialPreviewResponse resp = createDatasetFromSQL(TestWebSocket.LONG_TEST_QUERY, null);
    final InitialTransformAndRunResponse runResp = expectSuccess(
        getBuilder(
            getAPIv2()
                .path(versionedResourcePath(resp.getDataset()))
                .path("transformAndRun")
                .queryParam("newVersion", newVersion())
        ).buildPost(entity(new TransformSort("id", ASC), JSON)), InitialTransformAndRunResponse.class);
    JobId job = runResp.getJobId();
    NotificationResponse response = expectSuccess(
        getBuilder(
            getAPIv2()
                .path("job")
                .path(job.getId())
                .path("cancel")
        ).buildPost(entity(null, JSON)), NotificationResponse.class);
    assertEquals("Job cancellation requested", response.getMessage());
    assertEquals(NotificationResponse.ResponseType.OK, response.getType());
  }

  @Ignore
  @Test
  public void testJobService() throws Exception {
    populateInitialData();

    final DatasetPath ds1 = new DatasetPath("s.ds1");
    final DatasetPath ds2 = new DatasetPath("s.ds2");
    final DatasetPath ds3 = new DatasetPath("s.ds3");

    final CompletableFuture<Job> job1v0 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(),
      NoOpJobStatusListener.INSTANCE);
    final CompletableFuture<Job> job2v0 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(),
      NoOpJobStatusListener.INSTANCE);
    final CompletableFuture<Job> job3v0 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds3.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(),
      NoOpJobStatusListener.INSTANCE);

    JobsServiceUtil.waitForJobCompletion(job1v0);
    JobsServiceUtil.waitForJobCompletion(job2v0);
    JobsServiceUtil.waitForJobCompletion(job3v0);

    assertEquals(1, jobsService.getJobsCount(ds1.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds2.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds3.toNamespaceKey()));

    final CompletableFuture<Job> job1v2 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(),
      NoOpJobStatusListener.INSTANCE);
    final CompletableFuture<Job> job1v3 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build(),
      NoOpJobStatusListener.INSTANCE);
    final CompletableFuture<Job> job2v2 = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build(),
      NoOpJobStatusListener.INSTANCE);

    JobsServiceUtil.waitForJobCompletion(job1v2);
    JobsServiceUtil.waitForJobCompletion(job1v3);
    JobsServiceUtil.waitForJobCompletion(job2v2);

    assertEquals(3, jobsService.getJobsCount(ds1.toNamespaceKey()));
    assertEquals(2, jobsService.getJobsCount(ds2.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds3.toNamespaceKey()));

    assertEquals(2, jobsService.getJobsCountForDataset(ds1.toNamespaceKey(), new DatasetVersion("1")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds1.toNamespaceKey(), new DatasetVersion("2")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds2.toNamespaceKey(), new DatasetVersion("1")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds2.toNamespaceKey(), new DatasetVersion("2")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds3.toNamespaceKey(), new DatasetVersion("1")));

    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(),
        NoOpJobStatusListener.INSTANCE)
    );
    List<Job> jobs = ImmutableList.copyOf(jobsService.getAllJobs());
    assertEquals(7, jobs.size());

    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDatasetPath(ds1.toNamespaceKey())
        .build();
    jobs = ImmutableList.copyOf(jobsService.searchJobs(request));
    assertEquals(4, jobs.size());

    final SearchJobsRequest request1 = SearchJobsRequest.newBuilder()
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("1"))
        .build();
    jobs = ImmutableList.copyOf(jobsService.searchJobs(request1));
    assertEquals(3, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds1,ds==s.ds2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(6, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds3;dsv==v1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds3;dsv==v2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(0, jobs.size());
  }

  @Test
  public void testJobCompleted() throws Exception {
    populateInitialData();
    final DatasetPath ds1 = new DatasetPath("s.ds1");
    final CompletableFuture<Job> jobFuture = jobsService.submitJob(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1")).build(),
      NoOpJobStatusListener.INSTANCE);
    Job job = JobsServiceUtil.waitForJobCompletion(jobFuture);

    // get the latest version of the job entry
    GetJobRequest request = GetJobRequest.newBuilder()
      .setJobId(job.getJobId())
      .build();
    job = jobsService.getJob(request);
    // and make sure it's marked as completed
    assertTrue("job should be marked as 'completed'", job.isCompleted());
  }

  private Job createJob(final String id, final List<String> datasetPath, final String version, final String user,
                              final String space, final JobState state, final String sql,
                              final Long start, final Long end, QueryType queryType) {
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
            .setResourceSchedulingInfo(new ResourceSchedulingInfo().setQueueName("SMALL")
                                                                    .setRuleName("ruleSmall"));

    final JobAttempt jobAttempt =
        new JobAttempt()
        .setState(state)
        .setInfo(jobInfo);

    return new Job(jobId, jobAttempt);
  }

  @Test
  // DX-5119 Index unquoted dataset names along with quoted ones.
  // TODO (Amit H): DX-1563 We should be using analyzer to match both rather than indexing twice.
  public void testUnquotedJobFilter() throws Exception {
    Job jobA1 = createJob("A1", Arrays.asList("Prod-Sample", "ds-1"), "v1", "A", "Prod-Sample", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    jobsService.storeJob(jobA1);
    List<Job> jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ads==Prod-Sample.ds-1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==Prod-Sample.ds-1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(0, jobs.size());
  }

  @Test
  public void testJobManager() throws Exception {
    //
    //String id, final String ds, final String version, final String user,
    //final String space, final JobState state, final String sql,
    //final Long start, final Long end)
    int completed = 0;
    int running = 0;
    int canceled = 0;

    Job jobA1 = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    Job jobA2 = createJob("A2", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 250L, 260L, QueryType.UI_RUN);
    Job jobA3 = createJob("A3", Arrays.asList("space1", "ds1"), "v2", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 300L, 400L, QueryType.UI_RUN);
    Job jobA4 = createJob("A4", Arrays.asList("space1", "ds1"), "v3", "A", "space1", JobState.RUNNING, "select * from LocalFS1.\"dac-sample1.json\"", 100L, null, QueryType.UI_PREVIEW);
    Job jobA5 = createJob("A5", Arrays.asList("space1", "ds1"), "v2", "A", "space1", JobState.CANCELED, "select * from LocalFS1.\"dac-sample1.json\"", 300L, 301L, QueryType.UI_INTERNAL_PREVIEW);

    running += 1;
    completed += 3;
    canceled += 1;

    jobsService.storeJob(jobA1);
    jobsService.storeJob(jobA2);
    jobsService.storeJob(jobA3);
    jobsService.storeJob(jobA4);
    jobsService.storeJob(jobA5);

    Job jobB1 = createJob("B1", Arrays.asList("space1", "ds2"), "v1", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 120L, QueryType.UI_PREVIEW);
    Job jobB2 = createJob("B2", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 230L, 290L, QueryType.UI_PREVIEW);
    Job jobB3 = createJob("B3", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 300L, 400L, QueryType.UNKNOWN);
    Job jobB4 = createJob("B4", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 1000L, 2000L, QueryType.UNKNOWN);
    Job jobB5 = createJob("B5", Arrays.asList("space1", "ds2"), "v3", "B", "space1", JobState.RUNNING, "select * from LocalFS1.\"dac-sample1.json\"", 300L, null, QueryType.UI_INTERNAL_PREVIEW);

    running += 1;
    completed += 4;

    jobsService.storeJob(jobB1);
    jobsService.storeJob(jobB2);
    jobsService.storeJob(jobB3);
    jobsService.storeJob(jobB4);
    jobsService.storeJob(jobB5);

    Job jobC1 = createJob("C1", Arrays.asList("space2", "ds3"), "v1", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 400L, 500L, QueryType.UI_RUN);
    Job jobC2 = createJob("C2", Arrays.asList("space2", "ds3"), "v1", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 500L, 600L, QueryType.UI_RUN);
    Job jobC3 = createJob("C3", Arrays.asList("space2", "ds3"), "v2", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 600L, 700L, QueryType.UI_PREVIEW);

    completed += 3;
    jobsService.storeJob(jobC1);
    jobsService.storeJob(jobC2);
    jobsService.storeJob(jobC3);

    Job jobD1 = createJob("D1", Arrays.asList("space3","ds4"), "v4", "D", "space3", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 10L, 7000L, QueryType.REST);
    jobsService.storeJob(jobD1);
    completed += 1;

    // search by spaces
    List<Job> jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(10, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by query type
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_RUN")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_INTERNAL_PREVIEW")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_PREVIEW")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(4, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==REST")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UNKNOWN")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(9, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==EXTERNAL")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==ACCELERATION")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(0, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==INTERNAL")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(4, jobs.size());
    //TODO: uncomment after DX-2330 fix
    //jobs = jobsManager.getAllJobs("qt!=SCHEMA", null, null);
    //assertEquals(12, jobs.size());

    // search by users
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==A")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==B")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==C")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==D")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by job ids
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("job==A1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("job==B3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by dataset and version
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3;dsv==v1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3;dsv==v2")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space3.ds4")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space3.ds4;dsv==v4")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

//    // default sort order (descending by start time but running jobs on top)
//    // running jobs come first sorted by start time.
//    jobs = ((LocalJobsService)jobsService).getAllJobs();
//    assertEquals(jobB5.getJobId(), jobs.get(0).getJobId());
//    assertEquals(jobA4.getJobId(), jobs.get(1).getJobId());
//    assertEquals(jobB4.getJobId(), jobs.get(2).getJobId());
//    assertEquals(jobC3.getJobId(), jobs.get(3).getJobId());
//    assertEquals(jobC2.getJobId(), jobs.get(4).getJobId());

    // search by job state
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==COMPLETED")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==RUNNING")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(running, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==CANCELED")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(canceled, jobs.size());

    // filter by start and finish time
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=gt=0")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed + canceled, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=300;et=lt=1000")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(6, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());

    // SORT by start time
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setResultOrder(SearchJobsRequest.ResultOrder.ASCENDING)
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());
    assertEquals(jobD1.getJobId(), jobs.get(0).getJobId());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setResultOrder(SearchJobsRequest.ResultOrder.DESCENDING)
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());
    assertEquals(jobB4.getJobId(), jobs.get(0).getJobId());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=ge=0")
        .setSortColumn("et")
        .setResultOrder(SearchJobsRequest.ResultOrder.ASCENDING)
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(12, jobs.size());
    assertEquals(jobA1.getJobId(), jobs.get(0).getJobId());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=ge=0")
        .setSortColumn("et")
        .setResultOrder(SearchJobsRequest.ResultOrder.DESCENDING)
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(12, jobs.size());
    assertEquals(jobD1.getJobId(), jobs.get(0).getJobId());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("blah=contains=COMPLETED")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=ds3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space1")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(10, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space*.ds3")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space2.ds?")
        .setUsername(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    // user filtering
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setResultOrder(SearchJobsRequest.ResultOrder.ASCENDING)
        .setOffset(0)
        .setLimit(Integer.MAX_VALUE)
        .setUsername("A").build()));
    assertEquals(14, jobs.size());
  }

  @Test
  public void testJobParentSearch() throws Exception {

    Job jobA1 = createJob("A1", asList("space1", "ds1"), "v1", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    jobA1.getJobAttempt().getInfo().setFieldOriginsList(asList(
        new FieldOrigin("foo")
        .setOriginsList(asList(
            new Origin("foo", false)
            .setTableList(asList("LocalFS1", "dac-sample1.json"))
            ))
        ));
    jobsService.storeJob(jobA1);

    List<Job> jobsForParent = ImmutableList.copyOf(jobsService.getJobsForParent(new NamespaceKey(asList("LocalFS1", "dac-sample1.json")), Integer.MAX_VALUE));
    assertFalse(jobsForParent.isEmpty());
  }

  @Test
  public void testCTASAndDropTable() throws Exception {
    // Create a table
    SqlQuery ctas = getQueryFromSQL("CREATE TABLE \"$scratch\".\"ctas\" AS select * from cp.\"json/users.json\" LIMIT 1");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(ctas)
          .setQueryType(QueryType.UI_RUN)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    FileSystemPlugin plugin = (FileSystemPlugin) getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class).getSource("$scratch");

    // Make sure the table data files exist
    File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctas");
    assertTrue(ctasTableDir.exists());
    assertTrue(ctasTableDir.list().length >= 1);

    // Now drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctas\"");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(dropTable)
          .setQueryType(QueryType.ACCELERATOR_DROP)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    // Make sure the table data directory is deleted
    assertFalse(ctasTableDir.exists());
  }

  @Test
  public void testSingleCompletedAttempt() throws Exception {
    final String attemptId = AttemptIdUtils.toString(new AttemptId());

    Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    job.getJobAttempt().setDetails(new JobDetails());
    job.getJobAttempt().setAttemptId(attemptId);

    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), null, null, null, true, null, null);

    assertEquals("", detailsUI.getAttemptsSummary());
    assertEquals(1, detailsUI.getAttemptDetails().size());

    AttemptDetailsUI attemptDetailsUI = detailsUI.getAttemptDetails().get(0);
    assertEquals("", attemptDetailsUI.getReason());
    assertEquals(JobState.COMPLETED, attemptDetailsUI.getResult());
    assertEquals("/profiles/" + job.getJobId().getId() + "?attempt=0", attemptDetailsUI.getProfileUrl());
  }

  @Test
  public void testJobResultsCleanup() throws Exception {
    jobsService = (LocalJobsService) l(JobsService.class);
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    Job job = JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(
        JobRequest.newBuilder().setSqlQuery(ctas).build(), NoOpJobStatusListener.INSTANCE)
    );

    SabotContext context = l(SabotContext.class);
    OptionValue days = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 0);
    context.getOptionManager().setOption(days);
    OptionValue millis = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 10);
    context.getOptionManager().setOption(millis);

    Thread.sleep(20);

    LocalJobsService.JobResultsCleanupTask cleanupTask = jobsService.new JobResultsCleanupTask();
    cleanupTask.cleanup();

    //make sure that the job output directory is gone
    assertFalse(jobsService.getJobResultsStore().jobOutputDirectoryExists(job.getJobId()));
    GetJobRequest request = GetJobRequest.newBuilder()
      .setJobId(job.getJobId())
      .build();
    job = jobsService.getJob(request);
    assertFalse(JobDetailsUI.of(job).getResultsAvailable());

    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 30));
    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 0));
  }

  @Test
  public void testJobProfileCleanup() throws Exception {
    jobsService = (LocalJobsService) l(JobsService.class);
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    Job job = JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(ctas).build(), NoOpJobStatusListener.INSTANCE)
    );

    Thread.sleep(20);

    KVStoreProvider provider = l(KVStoreProvider.class);

    LocalJobsService.DeleteResult deleteResult = LocalJobsService.deleteOldJobs(provider, 10);
    assertEquals(1, deleteResult.getJobsDeleted());
    assertEquals(1, deleteResult.getProfilesDeleted());

    KVStore<AttemptId, UserBitShared.QueryProfile> profileStore = provider.getStore(LocalJobsService.JobsProfileCreator.class);
    UserBitShared.QueryProfile queryProfile = profileStore.get(AttemptIdUtils.fromString(job.getJobAttempt().getAttemptId()));
    assertEquals(null, queryProfile);

    thrown.expect(JobNotFoundException.class);
    GetJobRequest request = GetJobRequest.newBuilder()
      .setJobId(job.getJobId())
      .build();
    jobsService.getJob(request);
  }

  @Test
  public void testSingleFailedAttempt() throws Exception {
    final String attemptId = AttemptIdUtils.toString(new AttemptId());

    Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.FAILED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    job.getJobAttempt().setDetails(new JobDetails());
    job.getJobAttempt().setAttemptId(attemptId);

    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), new JobFailureInfo("Some error message", JobFailureType.UNKNOWN, null), null, null, false, null, null);

    assertEquals("", detailsUI.getAttemptsSummary());
    assertEquals(1, detailsUI.getAttemptDetails().size());

    AttemptDetailsUI attemptDetailsUI = detailsUI.getAttemptDetails().get(0);
    assertEquals("", attemptDetailsUI.getReason());
    assertEquals(JobState.FAILED, attemptDetailsUI.getResult());
    assertEquals("/profiles/" + job.getJobId().getId() + "?attempt=0", attemptDetailsUI.getProfileUrl());
  }

  @Test
  public void testMultipleAttempts() throws Exception {
    final UserBitShared.ExternalId externalId = ExternalIdHelper.generateExternalId();

    // 1st attempt OUT_OF_MEMORY
    final Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.FAILED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    AttemptId attemptId = AttemptId.of(externalId);
    job.getJobAttempt()
            .setAttemptId(AttemptIdUtils.toString(attemptId))
            .setState(JobState.FAILED)
            .setDetails(new JobDetails());

    // 3 more SCHEMA_CHANGE failures
    for (int i = 0; i < 3; i++) {
      attemptId = attemptId.nextAttempt();
      final JobAttempt jobAttempt = new JobAttempt()
              .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 100L+2*i, 100L+2*i+1, "failed"))
              .setAttemptId(AttemptIdUtils.toString(attemptId))
              .setState(JobState.FAILED)
              .setReason(i == 0 ? AttemptReason.OUT_OF_MEMORY : AttemptReason.SCHEMA_CHANGE)
              .setDetails(new JobDetails());
      job.addAttempt(jobAttempt);
    }

    // final attempt succeeds
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt = new JobAttempt()
            .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 106, 107, null))
            .setAttemptId(AttemptIdUtils.toString(attemptId))
            .setState(JobState.COMPLETED)
            .setReason(AttemptReason.SCHEMA_CHANGE)
            .setDetails(new JobDetails());
    job.addAttempt(jobAttempt);

    // retrieve the UI jobDetails
    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), new JobDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), null, null, null, false, null, null);

    assertEquals(JobState.COMPLETED, detailsUI.getState());
    assertEquals(Long.valueOf(100L), detailsUI.getStartTime());
    assertEquals(Long.valueOf(107L), detailsUI.getEndTime());
    assertNull(detailsUI.getFailureInfo());
    assertEquals(5, detailsUI.getAttemptDetails().size());
    assertEquals(AttemptsUIHelper.constructSummary(5, 1, 3), detailsUI.getAttemptsSummary());

    // check profileUrl
    attemptId = AttemptId.of(externalId);
    for (int i = 0; i < 5; i++, attemptId = attemptId.nextAttempt()) {
      final AttemptDetailsUI attemptDetails = detailsUI.getAttemptDetails().get(i);
      final String reason = i == 0 ? "" : (i == 1 ? AttemptsUIHelper.OUT_OF_MEMORY_TEXT : AttemptsUIHelper.SCHEMA_CHANGE_TEXT);
      checkAttemptDetail(attemptDetails, job.getJobId(), i, i == 4 ? JobState.COMPLETED : JobState.FAILED, reason);
    }
  }

  private static JobInfo newJobInfo(final JobInfo templateJobInfo, long start, long end, String failureInfo) {
    return new JobInfo(templateJobInfo.getJobId(), templateJobInfo.getSql(), templateJobInfo.getDatasetVersion(), templateJobInfo.getQueryType())
        .setSpace(templateJobInfo.getSpace())
        .setUser(templateJobInfo.getUser())
        .setStartTime(start)
        .setFinishTime(end)
        .setFailureInfo(failureInfo)
        .setDatasetPathList(templateJobInfo.getDatasetPathList());
  }

  private void checkAttemptDetail(AttemptDetailsUI attemptDetails, JobId jobId, int attemptNum, JobState state, String reason) {
    assertEquals("/profiles/" + jobId.getId() + "?attempt=" + attemptNum, attemptDetails.getProfileUrl());
    assertEquals(state, attemptDetails.getResult());
    assertEquals(reason, attemptDetails.getReason());
  }

  @Test
  public void testExplain() {
    final SqlQuery query = getQueryFromSQL("EXPLAIN PLAN FOR SELECT * FROM sys.version");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE)
    );
  }

  @Test
  public void testAlterOption() {
    final SqlQuery query = getQueryFromSQL("alter session set \"planner.enable_multiphase_agg\"=true");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE)
    );
  }

  @Test
  public void testAliasedQuery() throws Exception {
    final SqlQuery query = getQueryFromSQL("SHOW SCHEMAS");
    JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder().setSqlQuery(query).build(), NoOpJobStatusListener.INSTANCE)
    );
  }

  @Test
  public void testJobFilters() throws Exception {
    JobFilters jobFilters = new JobFilters()
        .addFilter(JobIndexKeys.START_TIME, 1200, 2000)
        .addContainsFilter("DG")
        .addFilter(JobIndexKeys.QUERY_TYPE, "UI", "EXTERNAL")
        .addFilter(JobIndexKeys.DATASET, "dsg10")
        .setSort(JobIndexKeys.END_TIME.getShortName(), SortOrder.ASCENDING);
    assertEquals("/jobs?filters=%7B%22st%22%3A%5B1200%2C2000%5D%2C%22contains%22%3A%5B%22DG%22%5D%2C%22qt%22%3A%5B%22UI%22%2C%22EXTERNAL%22%5D%2C%22ds%22%3A%5B%22dsg10%22%5D%7D&sort=et&order=ASCENDING", jobFilters.toUrl());
  }

  @Test
  public void testCTASReplace() throws Exception {
    NamespaceKey namespaceKey = new NamespaceKey("ctasSpace");
    SpaceConfig spaceConfig = new SpaceConfig();
    spaceConfig.setName("ctasSpace");

    newNamespaceService().addOrUpdateSpace(namespaceKey, spaceConfig);

    SqlQuery ctas = getQueryFromSQL("CREATE OR REPLACE VIEW ctasSpace.ctastest AS select * from (VALUES (1))");
    Job ctasJob = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(ctas)
          .setQueryType(QueryType.UI_RUN)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    waitForCompletion(ctasJob);

    ctas = getQueryFromSQL("CREATE OR REPLACE VIEW ctasSpace.ctastest AS select * from (VALUES (2))");
    ctasJob = Futures.getUnchecked(
      jobsService.submitJob(
        JobRequest.newBuilder()
          .setSqlQuery(ctas)
          .setQueryType(QueryType.UI_RUN)
          .build(),
        NoOpJobStatusListener.INSTANCE)
    );

    waitForCompletion(ctasJob);

    newNamespaceService().deleteSpace(namespaceKey, newNamespaceService().getSpace(namespaceKey).getTag());
  }

  @Test
  public void testMetadataAwaitingValidQuery() {
    Job job = JobsServiceUtil.waitForJobCompletion(
      jobsService.submitJob(JobRequest.newBuilder()
      .setSqlQuery(getQueryFromSQL("SELECT * FROM (VALUES(1234))"))
      .build(), NoOpJobStatusListener.INSTANCE));

    job.getData().waitForMetadata();
    assertEquals("batch schema is not empty after awaiting",true, job.getJobAttempt().getInfo().getBatchSchema() != null);
  }

  @Test
  public void testMetadataAwaitingInvalidQuery() {
    Job job = Futures.getUnchecked(
      jobsService.submitJob(JobRequest.newBuilder()
      .setSqlQuery(getQueryFromSQL("SELECT * FROM_1 (VALUES(1234))"))
      .build(), NoOpJobStatusListener.INSTANCE));

    // this is also tests that latch is released in case of error. waitForMetadata should not hang in that case
    job.getData().waitForMetadata();
    assertEquals("batch schema should be empty for invalid query",true, job.getJobAttempt().getInfo().getBatchSchema() == null);
  }

  private void waitForCompletion(Job job) throws Exception {
    while (true) {
      JobState state = job.getJobAttempt().getState();

      Assert.assertTrue("expected job to success successfully", Arrays.asList(JobState.PLANNING, JobState.RUNNING, JobState.ENQUEUED, JobState.STARTING, JobState.COMPLETED).contains(state));
      if (state == JobState.COMPLETED) {
        break;
      } else {
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
      }
    }
  }
}
