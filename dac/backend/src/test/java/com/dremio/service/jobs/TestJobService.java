/*
 * Copyright (C) 2017-2018 Dremio Corporation
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


import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.explore.model.InitialPreviewResponse;
import com.dremio.dac.explore.model.InitialTransformAndRunResponse;
import com.dremio.dac.model.job.AttemptDetailsUI;
import com.dremio.dac.model.job.AttemptsHelper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobFailureInfo;
import com.dremio.dac.model.job.JobFailureType;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.proto.model.dataset.TransformSort;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.resource.NotificationResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.server.socket.TestWebSocket;
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
import com.google.common.collect.ImmutableList;

/**
 * Tests for job service.
 */
public class TestJobService extends BaseTestServer {

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

    Job job1_0 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(), NoOpJobStatusListener.INSTANCE);
    Job job2_0 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(), NoOpJobStatusListener.INSTANCE);
    Job job3_0 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds3.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(), NoOpJobStatusListener.INSTANCE);

    job1_0.getData().loadIfNecessary();
    job2_0.getData().loadIfNecessary();
    job3_0.getData().loadIfNecessary();

    assertEquals(1, jobsService.getJobsCount(ds1.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds2.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds3.toNamespaceKey()));

    Job job1_2 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(), NoOpJobStatusListener.INSTANCE);
    Job job1_3 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build(), NoOpJobStatusListener.INSTANCE);
    Job job2_2 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build(), NoOpJobStatusListener.INSTANCE);

    job1_2.getData().loadIfNecessary();
    job1_3.getData().loadIfNecessary();
    job2_2.getData().loadIfNecessary();

    assertEquals(3, jobsService.getJobsCount(ds1.toNamespaceKey()));
    assertEquals(2, jobsService.getJobsCount(ds2.toNamespaceKey()));
    assertEquals(1, jobsService.getJobsCount(ds3.toNamespaceKey()));

    assertEquals(2, jobsService.getJobsCountForDataset(ds1.toNamespaceKey(), new DatasetVersion("1")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds1.toNamespaceKey(), new DatasetVersion("2")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds2.toNamespaceKey(), new DatasetVersion("1")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds2.toNamespaceKey(), new DatasetVersion("2")));
    assertEquals(1, jobsService.getJobsCountForDataset(ds3.toNamespaceKey(), new DatasetVersion("1")));

    Job job1_4 = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build(), NoOpJobStatusListener.INSTANCE);
    job1_4.getData().loadIfNecessary();
    List<Job> jobs = ImmutableList.copyOf(jobsService.getAllJobs());
    assertEquals(7, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.getJobsForDataset(ds1.toNamespaceKey(), Integer.MAX_VALUE));
    assertEquals(4, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.getJobsForDataset(ds1.toNamespaceKey(), new DatasetVersion("1"), Integer.MAX_VALUE));
    assertEquals(3, jobs.size());

    jobs = getAllJobs("ds==s.ds1,ds==s.ds2", null, null);
    assertEquals(6, jobs.size());

    jobs = getAllJobs("ds==s.ds3;dsv==v1", null, null);
    assertEquals(1, jobs.size());

    jobs = getAllJobs("ds==s.ds3;dsv==v2", null, null);
    assertEquals(0, jobs.size());
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
    List<Job> jobs = getAllJobs("ads==Prod-Sample.ds-1", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("ds==Prod-Sample.ds-1", null, null);
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
    List<Job> jobs = getAllJobs("spc==space1", null, null);
    assertEquals(10, jobs.size());
    jobs = getAllJobs("spc==space2", null, null);
    assertEquals(3, jobs.size());
    jobs = getAllJobs("spc==space3", null, null);
    assertEquals(1, jobs.size());

    // search by query type
    jobs = getAllJobs("qt==UI_RUN", null, null);
    assertEquals(5, jobs.size());
    jobs = getAllJobs("qt==UI_INTERNAL_PREVIEW", null, null);
    assertEquals(2, jobs.size());
    jobs = getAllJobs("qt==UI_PREVIEW", null, null);
    assertEquals(4, jobs.size());
    jobs = getAllJobs("qt==REST", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("qt==UNKNOWN", null, null);
    assertEquals(2, jobs.size());

    jobs = getAllJobs("qt==UI", null, null);
    assertEquals(9, jobs.size());
    jobs = getAllJobs("qt==EXTERNAL", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("qt==ACCELERATION", null, null);
    assertEquals(0, jobs.size());
    jobs = getAllJobs("qt==INTERNAL", null, null);
    assertEquals(4, jobs.size());
    //TODO: uncomment after DX-2330 fix
    //jobs = jobsManager.getAllJobs("qt!=SCHEMA", null, null);
    //assertEquals(12, jobs.size());

    // search by users
    jobs = getAllJobs("usr==A", null, null);
    assertEquals(5, jobs.size());
    jobs = getAllJobs("usr==B", null, null);
    assertEquals(5, jobs.size());
    jobs = getAllJobs("usr==C", null, null);
    assertEquals(3, jobs.size());
    jobs = getAllJobs("usr==D", null, null);
    assertEquals(1, jobs.size());

    // search by job ids
    jobs = getAllJobs("job==A1", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("job==B3", null, null);
    assertEquals(1, jobs.size());

    // search by dataset and version
    jobs = getAllJobs("ds==space1.ds1", null, null);
    assertEquals(5, jobs.size());
    jobs = getAllJobs("ds==space1.ds1;dsv==v1", null, null);
    assertEquals(2, jobs.size());
    jobs = getAllJobs("ds==space1.ds1;dsv==v2", null, null);
    assertEquals(2, jobs.size());
    jobs = getAllJobs("ds==space1.ds1;dsv==v3", null, null);
    assertEquals(1, jobs.size());

    jobs = getAllJobs("ds==space1.ds2", null, null);
    assertEquals(5, jobs.size());
    jobs = getAllJobs("ds==space1.ds2;dsv==v1", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("ds==space1.ds2;dsv==v2", null, null);
    assertEquals(3, jobs.size());
    jobs = getAllJobs("ds==space1.ds2;dsv==v3", null, null);
    assertEquals(1, jobs.size());

    jobs = getAllJobs("ds==space2.ds3", null, null);
    assertEquals(3, jobs.size());
    jobs = getAllJobs("ds==space2.ds3;dsv==v1", null, null);
    assertEquals(2, jobs.size());
    jobs = getAllJobs("ds==space2.ds3;dsv==v2", null, null);
    assertEquals(1, jobs.size());

    jobs = getAllJobs("ds==space3.ds4", null, null);
    assertEquals(1, jobs.size());
    jobs = getAllJobs("ds==space3.ds4;dsv==v4", null, null);
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
    jobs = getAllJobs("jst==COMPLETED", null, null);
    assertEquals(completed, jobs.size());

    jobs = getAllJobs("jst==RUNNING", null, null);
    assertEquals(running, jobs.size());

    jobs = getAllJobs("jst==CANCELED", null, null);
    assertEquals(canceled, jobs.size());

    // filter by start and finish time
    jobs = getAllJobs("et=gt=0", null, null);
    assertEquals(completed + canceled, jobs.size());

    jobs = getAllJobs("st=ge=300;et=lt=1000", null, null);
    assertEquals(6, jobs.size());

    jobs = getAllJobs("st=ge=0", "st", SortOrder.ASCENDING);
    assertEquals(14, jobs.size());

    // SORT by start time
    jobs = getAllJobs("st=ge=0", "st", SortOrder.ASCENDING);
    assertEquals(14, jobs.size());
    assertEquals(jobD1.getJobId(), jobs.get(0).getJobId());

    jobs = getAllJobs("st=ge=0", "st", SortOrder.DESCENDING);
    assertEquals(14, jobs.size());
    assertEquals(jobB4.getJobId(), jobs.get(0).getJobId());

    jobs = getAllJobs("et=ge=0", "et", SortOrder.ASCENDING);
    assertEquals(12, jobs.size());
    assertEquals(jobA1.getJobId(), jobs.get(0).getJobId());

    jobs = getAllJobs("et=ge=0", "et", SortOrder.DESCENDING);
    assertEquals(12, jobs.size());
    assertEquals(jobD1.getJobId(), jobs.get(0).getJobId());

    jobs = getAllJobs("blah=contains=COMPLETED", null, null);
    assertEquals(completed, jobs.size());

    jobs = getAllJobs("*=contains=ds3", null, null);
    assertEquals(3, jobs.size());

    jobs = getAllJobs("*=contains=space1", null, null);
    assertEquals(10, jobs.size());

    jobs = getAllJobs("*=contains=space*.ds3", null, null);
    assertEquals(3, jobs.size());

    jobs = getAllJobs("*=contains=space2.ds?", null, null);
    assertEquals(3, jobs.size());

    // user filtering
    jobs = ImmutableList.copyOf(jobsService.getAllJobs("st=ge=0", "st", SortOrder.ASCENDING, 0, Integer.MAX_VALUE, "A"));
    assertEquals(14, jobs.size());
  }

  private List<Job> getAllJobs(String filterString, String sortColumn, SortOrder sortOrder) {
    return ImmutableList.copyOf(jobsService.getAllJobs(filterString, sortColumn, sortOrder, 0, Integer.MAX_VALUE, DEFAULT_USERNAME));
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
    Job ctasJob = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .setQueryType(QueryType.UI_RUN)
        .build(), NoOpJobStatusListener.INSTANCE);
    ctasJob.getData().loadIfNecessary();

    FileSystemPlugin plugin = (FileSystemPlugin) getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class).getSource("$scratch");

    // Make sure the table data files exist
    File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctas");
    assertTrue(ctasTableDir.exists());
    assertTrue(ctasTableDir.list().length >= 1);

    // Now drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctas\"");
    Job dropTableJob = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(dropTable)
        .setQueryType(QueryType.ACCELERATOR_DROP)
        .build(), NoOpJobStatusListener.INSTANCE);
    dropTableJob.getData().loadIfNecessary();

    // Make sure the table data directory is deleted
    assertFalse(ctasTableDir.exists());
  }

  @Test
  public void testSingleCompletedAttempt() throws Exception {
    final String attemptId = AttemptIdUtils.toString(new AttemptId());

    Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    job.getJobAttempt().setDetails(new JobDetails());
    job.getJobAttempt().setAttemptId(attemptId);

    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), null, null, true, null);

    assertEquals("", detailsUI.getAttemptsSummary());
    assertEquals(1, detailsUI.getAttemptDetails().size());

    AttemptDetailsUI attemptDetailsUI = detailsUI.getAttemptDetails().get(0);
    assertEquals("", attemptDetailsUI.getReason());
    assertEquals(JobState.COMPLETED, attemptDetailsUI.getResult());
    assertEquals("/profiles/" + job.getJobId().getId() + "?attempt=0", attemptDetailsUI.getProfileUrl());
  }

  @Test
  public void testJobCleanup() throws Exception {
    jobsService = (LocalJobsService) l(JobsService.class);
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    Job job = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .build(), NoOpJobStatusListener.INSTANCE);
    job.getData().loadIfNecessary();

    SabotContext context = l(SabotContext.class);
    OptionValue days = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 0);
    context.getOptionManager().setOption(days);
    OptionValue millis = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 10);
    context.getOptionManager().setOption(millis);

    Thread.sleep(20);

    LocalJobsService.CleanupTask cleanupTask = jobsService.new CleanupTask();
    cleanupTask.cleanup();

    //make sure that the job output directory is gone
    assertFalse(jobsService.getJobResultsStore().jobOutputDirectoryExists(job.getJobId()));
    job = jobsService.getJob(job.getJobId());
    assertFalse(JobDetailsUI.of(job).getResultsAvailable());

    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 30));
    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 0));
  }

  @Test
  public void testSingleFailedAttempt() throws Exception {
    final String attemptId = AttemptIdUtils.toString(new AttemptId());

    Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.FAILED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    job.getJobAttempt().setDetails(new JobDetails());
    job.getJobAttempt().setAttemptId(attemptId);

    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), new JobFailureInfo("Some error message", JobFailureType.UNKNOWN, null), null, false, null);

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
            .setState(JobState.FAILED);

    // 3 more SCHEMA_CHANGE failures
    for (int i = 0; i < 3; i++) {
      attemptId = attemptId.nextAttempt();
      final JobAttempt jobAttempt = new JobAttempt()
              .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 100L+2*i, 100L+2*i+1, "failed"))
              .setAttemptId(AttemptIdUtils.toString(attemptId))
              .setState(JobState.FAILED)
              .setReason(i == 0 ? AttemptReason.OUT_OF_MEMORY : AttemptReason.SCHEMA_CHANGE);
      job.addAttempt(jobAttempt);
    }

    // final attempt succeeds
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt = new JobAttempt()
            .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 106, 107, null))
            .setAttemptId(AttemptIdUtils.toString(attemptId))
            .setState(JobState.COMPLETED)
            .setReason(AttemptReason.SCHEMA_CHANGE);
    job.addAttempt(jobAttempt);

    // retrieve the UI jobDetails
    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), new JobDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(job), null, null, false, null);

    assertEquals(JobState.COMPLETED, detailsUI.getState());
    assertEquals(Long.valueOf(100L), detailsUI.getStartTime());
    assertEquals(Long.valueOf(107L), detailsUI.getEndTime());
    assertNull(detailsUI.getFailureInfo());
    assertEquals(5, detailsUI.getAttemptDetails().size());
    assertEquals(AttemptsHelper.constructSummary(5, 1, 3), detailsUI.getAttemptsSummary());

    // check profileUrl
    attemptId = AttemptId.of(externalId);
    for (int i = 0; i < 5; i++, attemptId = attemptId.nextAttempt()) {
      final AttemptDetailsUI attemptDetails = detailsUI.getAttemptDetails().get(i);
      final String reason = i == 0 ? "" : (i == 1 ? AttemptsHelper.OUT_OF_MEMORY_TEXT : AttemptsHelper.SCHEMA_CHANGE_TEXT);
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
  public void testExplain() throws Exception {
    SqlQuery ctas = getQueryFromSQL("EXPLAIN PLAN FOR SELECT * FROM sys.version");
    Job ctasJob = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .build(), NoOpJobStatusListener.INSTANCE);
    ctasJob.getData().loadIfNecessary();
  }

  @Test
  public void testAlterOption() throws Exception {
    SqlQuery ctas = getQueryFromSQL("alter session set \"planner.enable_multiphase_agg\"=true");
    Job ctasJob = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .build(), NoOpJobStatusListener.INSTANCE);
    ctasJob.getData().loadIfNecessary();
  }

  @Test
  public void testAliasedQuery() throws Exception {
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    Job ctasJob = jobsService.submitJob(JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .build(), NoOpJobStatusListener.INSTANCE);
    ctasJob.getData().loadIfNecessary();
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
}
