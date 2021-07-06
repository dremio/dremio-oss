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

import static com.dremio.dac.server.JobsServiceTestUtils.toSubmitJobRequest;
import static com.dremio.exec.ExecConstants.MAX_FOREMEN_PER_COORDINATOR;
import static com.dremio.exec.testing.ExecutionControls.DEFAULT_CONTROLS;
import static com.dremio.exec.work.foreman.AttemptManager.INJECTOR_DURING_PLANNING_PAUSE;
import static com.dremio.exec.work.foreman.AttemptManager.INJECTOR_PLAN_PAUSE;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static java.util.Arrays.asList;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.client.Entity.entity;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.common.utils.protos.AttemptId;
import com.dremio.common.utils.protos.AttemptIdUtils;
import com.dremio.common.utils.protos.ExternalIdHelper;
import com.dremio.common.utils.protos.QueryIdHelper;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.model.job.AttemptDetailsUI;
import com.dremio.dac.model.job.AttemptsUIHelper;
import com.dremio.dac.model.job.JobDetailsUI;
import com.dremio.dac.model.job.JobFailureInfo;
import com.dremio.dac.model.job.JobFailureType;
import com.dremio.dac.model.job.JobFilters;
import com.dremio.dac.model.job.ResultOrder;
import com.dremio.dac.resource.JobResource;
import com.dremio.dac.resource.NotificationResponse;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.datastore.SearchTypes.SortOrder;
import com.dremio.datastore.api.LegacyKVStore;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.planner.DremioHepPlanner;
import com.dremio.exec.planner.DremioVolcanoPlanner;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.beans.AttemptEvent;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.CatalogService;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ExecutionControls;
import com.dremio.exec.testing.Injection;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.exec.work.protector.ForemenWorkManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.proto.model.attempts.AttemptReason;
import com.dremio.sabot.exec.CancelQueryContext;
import com.dremio.sabot.exec.CoordinatorHeapClawBackStrategy;
import com.dremio.service.job.JobCountsRequest;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobsWithParentDatasetRequest;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobDetails;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.job.proto.ResourceSchedulingInfo;
import com.dremio.service.jobs.LocalJobsService.OnlineProfileCleanup;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.dataset.DatasetVersion;
import com.dremio.service.namespace.dataset.proto.FieldOrigin;
import com.dremio.service.namespace.dataset.proto.Origin;
import com.dremio.service.namespace.space.proto.SpaceConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

/**
 * Tests for job service.
 */
public class TestJobService extends BaseTestServer {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  private HybridJobsService jobsService;
  private LocalJobsService localJobsService;
  private ForemenWorkManager foremenWorkManager;

  @Before
  public void setup() throws Exception {
    clearAllDataExceptUser();
    jobsService = (HybridJobsService) l(JobsService.class);
    localJobsService = l(LocalJobsService.class);
    foremenWorkManager = l(ForemenWorkManager.class);
  }

  private com.dremio.service.job.JobDetails getJobDetails(Job job) {
    JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
      .build();
    try {
      return jobsService.getJobDetails(request);
    } catch (JobNotFoundException e) {
      throw new IllegalArgumentException("Job Not Found", e);
    }
  }

  public static void failFunction() {
    throw UserException.dataReadError().message("expected failure").buildSilently();
  }

  // Test cancelling query past the planning phase.
  @Test
  public void testCancel() throws Exception {
    final JobSubmittedListener jobSubmittedListener = new JobSubmittedListener();
    final CompletionListener completionListener = new CompletionListener();
    final String testKey = TestingFunctionHelper.newKey(() -> {});

    final JobRequest request = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(String.format("SELECT WAIT(key, 5) FROM (VALUES('%s')) tbl(key)", testKey), null, DEFAULT_USERNAME))
      .build();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), new MultiJobStatusListener(completionListener, jobSubmittedListener));
    jobSubmittedListener.await();

    // sleep until query reaches starting state and then cancel query below.
    sleepUntilQueryState(jobId, AttemptEvent.State.STARTING);

    NotificationResponse response = expectSuccess(
      getBuilder(
        getAPIv2()
          .path("job")
          .path(jobId.getId())
          .path("cancel")
      ).buildPost(entity(null, JSON)), NotificationResponse.class);
    completionListener.await();

    assertEquals("Job cancellation requested", response.getMessage());
    assertEquals(NotificationResponse.ResponseType.OK, response.getType());
  }

  // Sleep until query state reaches given state
  private void sleepUntilQueryState(JobId jobId, AttemptEvent.State state) throws Exception {
    while (true) {
      Thread.sleep(100);
      Job job = getJob(jobId);
      final JobAttempt jobAttempt = job.getJobAttempt();
      long count;
      synchronized (jobAttempt) {
        count = jobAttempt
          .getStateListList()
          .stream()
          .map(e -> e.getState())
          .filter(s -> state.equals(s))
          .count();
      }
      if (count == 1) {
        break;
      }
    }
  }

  private void injectPauses(String controls) throws Exception {
    ObjectMapper objectMapper = spy(new ObjectMapper());
    objectMapper.addMixInAnnotations(Injection.class, ExecutionControls.InjectionMixIn.class);
    ExecutionControls.Controls execControls = objectMapper.readValue(controls, ExecutionControls.Controls.class);
    Mockito.doReturn(execControls).when(objectMapper).readValue(DEFAULT_CONTROLS, ExecutionControls.Controls.class);
    ExecutionControls.setControlsOptionMapper(objectMapper);
  }

  private Job getJob(JobId jobId) throws Exception {
    GetJobRequest getJobRequest = GetJobRequest.newBuilder()
                                               .setJobId(jobId)
                                               .setUserName(SYSTEM_USERNAME)
                                               .build();
    return localJobsService.getJob(getJobRequest);
  }

  /**
   * Test cancelling of query/job in planning
   */
  @Test
  public void testCancelPlanning() throws Exception {
    try {
      String controls = Controls.newBuilder()
                                .addPause(DremioVolcanoPlanner.class, INJECTOR_DURING_PLANNING_PAUSE)
                                .addPause(DremioHepPlanner.class, INJECTOR_DURING_PLANNING_PAUSE)
                                .build();
      injectPauses(controls);
      AttemptEvent.State[] observedAttemptStates = executeQueryAndCancel(true);
      AttemptEvent.State[] expectedAttemptStates =
        new AttemptEvent.State[] { AttemptEvent.State.PENDING,
                                   AttemptEvent.State.METADATA_RETRIEVAL,
                                   AttemptEvent.State.PLANNING,
                                   AttemptEvent.State.FAILED };

      assertArrayEquals("Since we paused during planning, there should be AttemptEvent.State.PLANNING" +
                        " before AttemptEvent.State.FAILED.",
                        expectedAttemptStates, observedAttemptStates);
    } catch(Exception e) {
      throw e;
    } finally {
      // reset, irrespective any exception, so that other test cases are not affected.
      ExecutionControls.setControlsOptionMapper(new ObjectMapper());
    }
  }

  /**
   * Test cancelling of query/job by heap monitor after planning phase.
   * Query should NOT be canceled since heap monitor should only cancel queries
   * in planning phase.
   */
  @Test
  public void testQueryCancelByHeapMonitorAfterPlanning() throws Exception {
    try {
      String controls = Controls.newBuilder()
                                .addPause(AttemptManager.class, INJECTOR_PLAN_PAUSE)
                                .build();
      injectPauses(controls);
      AttemptEvent.State[] observedAttemptStates = executeQueryAndCancel(false);
      assertEquals("Query should be completed successfully if heap monitor tried to cancel " +
                   "query after planning phase.",
                   AttemptEvent.State.COMPLETED,
                   observedAttemptStates[observedAttemptStates.length-1]);
    } catch(Exception e) {
      throw e;
    } finally {
      // reset, irrespective any exception, so that other test cases are not affected.
      ExecutionControls.setControlsOptionMapper(new ObjectMapper());
    }
  }


  private AttemptEvent.State[] executeQueryAndCancel(boolean verifyFailed) throws Exception {
    final JobSubmittedListener jobSubmittedListener = new JobSubmittedListener();
    final CompletionListener completionListener = new CompletionListener();
    final String testKey = TestingFunctionHelper.newKey(() -> {});

    SqlQuery sqlQuery = new SqlQuery(String.format("SELECT WAIT(key, 5) FROM (VALUES('%s')) tbl(key)", testKey),
                                     null, DEFAULT_USERNAME);
    final JobRequest request = JobRequest.newBuilder()
                                         .setSqlQuery(sqlQuery)
                                         .build();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request),
                                              new MultiJobStatusListener(completionListener, jobSubmittedListener));
    UserBitShared.ExternalId externalId = ExternalIdHelper.toExternal(QueryIdHelper.getQueryIdFromString(jobId.getId()));

    GetJobRequest getJobRequest = GetJobRequest.newBuilder()
                                               .setJobId(jobId)
                                               .setUserName(SYSTEM_USERNAME)
                                               .build();
    jobSubmittedListener.await();

    // wait for the injected pause to be hit
    Thread.sleep(3000);

    CancelQueryContext cancelQueryContext = CoordinatorHeapClawBackStrategy.getCancelQueryContext();
    String cancelReason = cancelQueryContext.getCancelReason();
    String cancelContext = cancelQueryContext.getCancelContext();
    // cancel query in planning phase
    foremenWorkManager.cancel(cancelQueryContext);

    // resume the pause
    foremenWorkManager.resume(externalId);

    // Observed exception
    Exception observedEx = null;

    // wait for cancel to take effect.
    try {
      completionListener.await();
    } catch (Exception e) {
      observedEx = e;
    }

    Job job = localJobsService.getJob(getJobRequest);
    if (verifyFailed) {
      assertTrue(UserRemoteException.class.equals(observedEx.getClass()));
      UserBitShared.DremioPBError error = ((UserRemoteException) observedEx).getOrCreatePBError(false);
      assertEquals(cancelReason, error.getOriginalMessage());
      assertEquals(l(SabotContext.class).getEndpoint(), error.getEndpoint());
      assertTrue("Cancel context should be in returned error context", error.getContextList().contains(cancelContext));

      assertEquals("Job is expected to be in FAILED state",
                   JobState.FAILED,
                   job.getJobAttempt().getState());
    }

    AttemptEvent.State[] observedAttemptStates = job.getJobAttempt()
                                                    .getStateListList()
                                                    .stream()
                                                    .map(e->e.getState())
                                                    .collect(Collectors.toList())
                                                    .toArray(new AttemptEvent.State[0]);
    return observedAttemptStates;
  }


  @Test
  public void testJobPlanningTime() throws Exception {
    final UserBitShared.ExternalId externalId = ExternalIdHelper.generateExternalId();

    // first attempt is FAILED
    final Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.FAILED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    AttemptId attemptId = AttemptId.of(externalId);
    job.getJobAttempt()
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.FAILED)
      .setDetails(new JobDetails());

    // second attempt is STARTING
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt1 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.STARTING)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt1);

    // third attempt is CANCELED
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt2 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.CANCELED)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt2);

    // fourth attempt is FAILED
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt3 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.FAILED)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt3);

    // fifth attempt is RUNNING
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt4 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.RUNNING)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt4);

    // sixth attempt is COMPLETED
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt5 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.COMPLETED)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt5);

    // seventh attempt is PLANNING
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt6 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.PLANNING)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt6);

    // eighth attempt is CANCELLATION_REQUESTED
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt7 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.CANCELLATION_REQUESTED)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt7);

    // final attempt is ENQUEUED
    attemptId = attemptId.nextAttempt();
    final JobAttempt jobAttempt8 = new JobAttempt()
      .setInfo(newJobInfo(job.getJobAttempt().getInfo(), 10000, 0, null, System.currentTimeMillis(), 0L))
      .setAttemptId(AttemptIdUtils.toString(attemptId))
      .setState(JobState.ENQUEUED)
      .setDetails(new JobDetails());
    job.addAttempt(jobAttempt8);

    localJobsService.storeJob(job);

    // retrieve the UI jobDetails
    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), new JobDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(getJobDetails(job)), null, null, null, false, null, null);

    assertEquals("Enqueued time of second attempt was not 0.", (long)detailsUI.getAttemptDetails().get(1).getQueuedTime(), 0L);
    assertEquals("Planning time of second attempt was not 0.", (long)detailsUI.getAttemptDetails().get(1).getPlanningTime(), 0L);
    assertEquals("Enqueued time of third attempt was not 0.", (long)detailsUI.getAttemptDetails().get(2).getQueuedTime(), 0L);
    assertEquals("Planning time of third attempt was not 0.", (long)detailsUI.getAttemptDetails().get(2).getPlanningTime(), 0L);
    assertEquals("Enqueued time of fourth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(3).getQueuedTime(), 0L);
    assertEquals("Planning time of fourth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(3).getPlanningTime(), 0L);
    assertEquals("Enqueued time of fifth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(4).getQueuedTime(), 0L);
    assertEquals("Planning time of fifth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(4).getPlanningTime(), 0L);
    assertEquals("Enqueued time of sixth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(5).getQueuedTime(), 0L);
    assertEquals("Planning time of sixth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(5).getPlanningTime(), 0L);
    assertEquals("Enqueued time of seventh attempt was not 0.", (long)detailsUI.getAttemptDetails().get(6).getQueuedTime(), 0L);
    assertEquals("Planning time of seventh attempt was not 0.", (long)detailsUI.getAttemptDetails().get(6).getPlanningTime(), 0L);
    assertEquals("Enqueued time of eighth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(7).getQueuedTime(), 0L);
    assertEquals("Planning time of eighth attempt was not 0.", (long)detailsUI.getAttemptDetails().get(7).getPlanningTime(), 0L);
    assertTrue("Enqueued time of final attempt was less than 0.", detailsUI.getAttemptDetails().get(8).getQueuedTime() >= 0);
    assertEquals("Planning time of final attempt was not 0.", (long)detailsUI.getAttemptDetails().get(8).getPlanningTime(), 0L);
  }

  @Ignore
  @Test
  public void testJobService() throws Exception {
    populateInitialData();

    final DatasetPath ds1 = new DatasetPath("s.ds1");
    final DatasetPath ds2 = new DatasetPath("s.ds2");
    final DatasetPath ds3 = new DatasetPath("s.ds3");

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build()
    );
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build()
    );
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds3.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build()
    );

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds3.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build()
    );
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build()
    );
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds2.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v2")).build()
    );

    assertEquals(3, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds1.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(2, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds2.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds3.toNamespaceKey().getPathComponents()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(2, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds1.toNamespaceKey().getPathComponents())
            .setVersion(new DatasetVersion("1").getVersion()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds1.toNamespaceKey().getPathComponents())
            .setVersion(new DatasetVersion("2").getVersion()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds2.toNamespaceKey().getPathComponents())
            .setVersion(new DatasetVersion("1").getVersion()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds2.toNamespaceKey().getPathComponents())
            .setVersion(new DatasetVersion("2").getVersion()))
        .build())
        .getCountList()
        .get(0));

    assertEquals(1, (int) jobsService.getJobCounts(JobCountsRequest.newBuilder()
        .addDatasets(VersionedDatasetPath.newBuilder()
            .addAllPath(ds3.toNamespaceKey().getPathComponents())
            .setVersion(new DatasetVersion("1").getVersion()))
        .build())
        .getCountList()
        .get(0));

    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .setDatasetPath(ds1.toNamespaceKey())
        .setDatasetVersion(new DatasetVersion("v1")).build()
    );
    List<Job> jobs = ImmutableList.copyOf(localJobsService.getAllJobs());
    assertEquals(7, jobs.size());

    final SearchJobsRequest request = SearchJobsRequest.newBuilder()
        .setDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toPathList())
          .build())
        .build();
    List<JobSummary> jobSummaries = ImmutableList.copyOf(jobsService.searchJobs(request));
    assertEquals(4, jobSummaries.size());

    final SearchJobsRequest request1 = SearchJobsRequest.newBuilder()
      .setDataset(VersionedDatasetPath.newBuilder()
        .addAllPath(ds1.toPathList())
        .setVersion(new DatasetVersion("1").getVersion())
        .build())
      .build();
    jobSummaries = ImmutableList.copyOf(jobsService.searchJobs(request1));
    assertEquals(3, jobSummaries.size());

    jobSummaries = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds1,ds==s.ds2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(6, jobSummaries.size());

    jobSummaries = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds3;dsv==v1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobSummaries.size());

    jobSummaries = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==s.ds3;dsv==v2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(0, jobSummaries.size());
  }

  @Test
  public void testJobCompleted() throws Exception {
    populateInitialData();
    final DatasetPath ds1 = new DatasetPath("s.ds1");
    final JobId jobId = submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 1"))
        .build()
    );

    // get the latest version of the job entry
    final JobDetailsRequest request1 = JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setProvideResultInfo(true)
        .build();
    final com.dremio.service.job.JobDetails jobDetails1 = jobsService.getJobDetails(request1);
    // and make sure it's marked as completed
    assertTrue("job should be marked as 'completed'", jobDetails1.getCompleted());
    assertTrue(jobDetails1.getHasResults());
    assertFalse(jobDetails1.getJobResultTableName().isEmpty());

    final JobDetailsRequest request2 = JobDetailsRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .build();
    final com.dremio.service.job.JobDetails jobDetails2 = jobsService.getJobDetails(request2);
    // and make sure it's marked as completed
    assertTrue("job should be marked as 'completed'", jobDetails2.getCompleted());
    assertFalse(jobDetails2.getHasResults()); // although results are available, request did not ask for the info
    assertTrue(jobDetails2.getJobResultTableName().isEmpty());
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
    localJobsService.storeJob(jobA1);
    List<JobSummary> jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ads==Prod-Sample.ds-1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==Prod-Sample.ds-1")
        .setUserName(DEFAULT_USERNAME)
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

    localJobsService.storeJob(jobA1);
    localJobsService.storeJob(jobA2);
    localJobsService.storeJob(jobA3);
    localJobsService.storeJob(jobA4);
    localJobsService.storeJob(jobA5);

    Job jobB1 = createJob("B1", Arrays.asList("space1", "ds2"), "v1", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 120L, QueryType.UI_PREVIEW);
    Job jobB2 = createJob("B2", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 230L, 290L, QueryType.UI_PREVIEW);
    Job jobB3 = createJob("B3", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 300L, 400L, QueryType.UNKNOWN);
    Job jobB4 = createJob("B4", Arrays.asList("space1", "ds2"), "v2", "B", "space1", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 1000L, 2000L, QueryType.UNKNOWN);
    Job jobB5 = createJob("B5", Arrays.asList("space1", "ds2"), "v3", "B", "space1", JobState.RUNNING, "select * from LocalFS1.\"dac-sample1.json\"", 300L, null, QueryType.UI_INTERNAL_PREVIEW);

    running += 1;
    completed += 4;

    localJobsService.storeJob(jobB1);
    localJobsService.storeJob(jobB2);
    localJobsService.storeJob(jobB3);
    localJobsService.storeJob(jobB4);
    localJobsService.storeJob(jobB5);

    Job jobC1 = createJob("C1", Arrays.asList("space2", "ds3"), "v1", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 400L, 500L, QueryType.UI_RUN);
    Job jobC2 = createJob("C2", Arrays.asList("space2", "ds3"), "v1", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 500L, 600L, QueryType.UI_RUN);
    Job jobC3 = createJob("C3", Arrays.asList("space2", "ds3"), "v2", "C", "space2", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 600L, 700L, QueryType.UI_PREVIEW);

    completed += 3;
    localJobsService.storeJob(jobC1);
    localJobsService.storeJob(jobC2);
    localJobsService.storeJob(jobC3);

    Job jobD1 = createJob("D1", Arrays.asList("space3","ds4"), "v4", "D", "space3", JobState.COMPLETED, "select * from LocalFS1.\"dac-sample1.json\"", 10L, 7000L, QueryType.REST);
    localJobsService.storeJob(jobD1);
    completed += 1;

    // search by spaces
    List<JobSummary> jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(10, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("spc==space3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by query type
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_RUN")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_INTERNAL_PREVIEW")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI_PREVIEW")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(4, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==REST")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UNKNOWN")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==UI")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(9, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==EXTERNAL")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==ACCELERATION")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(0, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("qt==INTERNAL")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(4, jobs.size());
    //TODO: uncomment after DX-2330 fix
    //jobs = jobsManager.getAllJobs("qt!=SCHEMA", null, null);
    //assertEquals(12, jobs.size());

    // search by users
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==A")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==B")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==C")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("usr==D")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by job ids
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("job==A1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("job==B3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    // search by dataset and version
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds1;dsv==v3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(5, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space1.ds2;dsv==v3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3;dsv==v1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(2, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space2.ds3;dsv==v2")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space3.ds4")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(1, jobs.size());
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("ds==space3.ds4;dsv==v4")
        .setUserName(DEFAULT_USERNAME)
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
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==RUNNING")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(running, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("jst==CANCELED")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(canceled, jobs.size());

    // filter by start and finish time
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=gt=0")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed + canceled, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=300;et=lt=1000")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(6, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());

    // SORT by start time
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setSortOrder(ResultOrder.ASCENDING.toSortOrder())
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());
    assertEquals(jobD1.getJobId(), JobsProtoUtil.toStuff(jobs.get(0).getJobId()));

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(14, jobs.size());
    assertEquals(jobB4.getJobId(), JobsProtoUtil.toStuff(jobs.get(0).getJobId()));

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=ge=0")
        .setSortColumn("et")
        .setSortOrder(ResultOrder.ASCENDING.toSortOrder())
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(12, jobs.size());
    assertEquals(jobA1.getJobId(), JobsProtoUtil.toStuff(jobs.get(0).getJobId()));

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("et=ge=0")
        .setSortColumn("et")
        .setSortOrder(ResultOrder.DESCENDING.toSortOrder())
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(12, jobs.size());
    assertEquals(jobD1.getJobId(), JobsProtoUtil.toStuff(jobs.get(0).getJobId()));

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("blah=contains=COMPLETED")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(completed, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=ds3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space1")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(10, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space*.ds3")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("*=contains=space2.ds?")
        .setUserName(DEFAULT_USERNAME)
        .build()));
    assertEquals(3, jobs.size());

    // user filtering
    jobs = ImmutableList.copyOf(jobsService.searchJobs(SearchJobsRequest.newBuilder()
        .setFilterString("st=ge=0")
        .setSortColumn("st")
        .setSortOrder(ResultOrder.ASCENDING.toSortOrder())
        .setOffset(0)
        .setLimit(Integer.MAX_VALUE)
        .setUserName("A").build()));
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
    localJobsService.storeJob(jobA1);

    JobsWithParentDatasetRequest jobsWithParentDatasetRequest = JobsWithParentDatasetRequest.newBuilder()
      .setDataset(VersionedDatasetPath.newBuilder()
        .addAllPath(asList("LocalFS1", "dac-sample1.json")))
      .setLimit(Integer.MAX_VALUE)
      .build();
    List<com.dremio.service.job.JobDetails> jobsForParent = ImmutableList.copyOf(jobsService.getJobsForParent(jobsWithParentDatasetRequest));
    assertFalse(jobsForParent.isEmpty());
  }

  @Test
  public void testCTASAndDropTable() throws Exception {
    // Create a table
    SqlQuery ctas = getQueryFromSQL("CREATE TABLE \"$scratch\".\"ctas\" AS select * from cp.\"json/users.json\" LIMIT 1");
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    FileSystemPlugin plugin = (FileSystemPlugin) getCurrentDremioDaemon().getBindingProvider().lookup(CatalogService.class).getSource("$scratch");

    // Make sure the table data files exist
    File ctasTableDir = new File(plugin.getConfig().getPath().toString(), "ctas");
    assertTrue(ctasTableDir.exists());
    assertTrue(ctasTableDir.list().length >= 1);

    // Now drop the table
    SqlQuery dropTable = getQueryFromSQL("DROP TABLE \"$scratch\".\"ctas\"");
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(dropTable)
        .setQueryType(QueryType.ACCELERATOR_DROP)
        .build()
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
    localJobsService.storeJob(job);

    JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(job.getJobId()))
      .build();
    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(jobsService.getJobDetails(request)), null, null, null, true, null, null);

    assertEquals("", detailsUI.getAttemptsSummary());
    assertEquals(1, detailsUI.getAttemptDetails().size());

    AttemptDetailsUI attemptDetailsUI = detailsUI.getAttemptDetails().get(0);
    assertEquals("", attemptDetailsUI.getReason());
    assertEquals(JobState.COMPLETED, attemptDetailsUI.getResult());
    assertEquals("/profiles/" + job.getJobId().getId() + "?attempt=0", attemptDetailsUI.getProfileUrl());
  }

  @Test
  public void testJobResultsCleanup() throws Exception {
    jobsService = (HybridJobsService) l(JobsService.class);
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    final JobId jobId = submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(ctas).build());

    SabotContext context = l(SabotContext.class);
    OptionValue days = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 0);
    context.getOptionManager().setOption(days);
    OptionValue millis = OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 10);
    context.getOptionManager().setOption(millis);

    Thread.sleep(20);

    LocalJobsService.JobResultsCleanupTask cleanupTask = localJobsService.createCleanupTask();
    cleanupTask.cleanup();

    //make sure that the job output directory is gone
    assertFalse(localJobsService.getJobResultsStore().jobOutputDirectoryExists(jobId));
    JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .build();
    com.dremio.service.job.JobDetails jobDetails = jobsService.getJobDetails(request);
    assertFalse(JobDetailsUI.of(jobDetails, jobDetails.getAttempts(0).getInfo().getUser()).getResultsAvailable());

    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.RESULTS_MAX_AGE_IN_DAYS.getOptionName(), 30));
    context.getOptionManager().setOption(OptionValue.createLong(OptionType.SYSTEM, ExecConstants.DEBUG_RESULTS_MAX_AGE_IN_MILLISECONDS.getOptionName(), 0));
  }

  @Test
  public void testJobProfileCleanup() throws Exception {
    jobsService = (HybridJobsService) l(JobsService.class);
    SqlQuery ctas = getQueryFromSQL("SHOW SCHEMAS");
    final JobId jobId = submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(ctas).build());
    final com.dremio.service.job.JobDetails jobDetails = jobsService.getJobDetails(JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());

    Thread.sleep(20);

    LegacyKVStoreProvider provider = l(LegacyKVStoreProvider.class);

    final OnlineProfileCleanup onlineProfileCleanup = l(LocalJobsService.class).new OnlineProfileCleanup();
    List<Long> deleteResult =
      l(LocalJobsService.class).deleteOldJobsAndProfiles(onlineProfileCleanup, provider ,10);
    assertTrue("Expect deleted 1 job, but was " + deleteResult.get(0),1 == deleteResult.get(0));
    assertTrue("Expect deleted 1 profile, but was " + deleteResult.get(1), 1 == deleteResult.get(1));
    assertTrue("Expect 0 failure, but was " + deleteResult.get(1), 0 == deleteResult.get(2));

    LegacyKVStore<AttemptId, UserBitShared.QueryProfile> profileStore =
      provider.getStore(LocalProfileStore.KVProfileStoreCreator.class);
    UserBitShared.QueryProfile queryProfile = profileStore.get(AttemptIdUtils.fromString(JobsProtoUtil.getLastAttempt(jobDetails).getAttemptId()));
    assertEquals(null, queryProfile);

    thrown.expect(JobNotFoundException.class);
    JobDetailsRequest request = JobDetailsRequest.newBuilder()
      .setJobId(jobDetails.getJobId())
      .build();
    jobsService.getJobDetails(request);
  }

  @Test
  public void testSingleFailedAttempt() throws Exception {
    final String attemptId = AttemptIdUtils.toString(new AttemptId());

    Job job = createJob("A1", Arrays.asList("space1", "ds1"), "v1", "A", "space1", JobState.FAILED, "select * from LocalFS1.\"dac-sample1.json\"", 100L, 110L, QueryType.UI_RUN);
    job.getJobAttempt().setDetails(new JobDetails());
    job.getJobAttempt().setAttemptId(attemptId);
    localJobsService.storeJob(job);

    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), job.getJobAttempt().getDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(getJobDetails(job)), new JobFailureInfo("Some error message", JobFailureType.UNKNOWN, null), null, null, false, null, null);

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
    localJobsService.storeJob(job);

    // retrieve the UI jobDetails
    JobDetailsUI detailsUI = new JobDetailsUI(job.getJobId(), new JobDetails(), JobResource.getPaginationURL(job.getJobId()), job.getAttempts(), JobResource.getDownloadURL(getJobDetails(job)), null, null, null, false, null, null);

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

  private static JobInfo newJobInfo(final JobInfo templateJobInfo, long start, long end, String failureInfo, long schedulingStart, long schedulingEnd) {
    return new JobInfo(templateJobInfo.getJobId(), templateJobInfo.getSql(), templateJobInfo.getDatasetVersion(), templateJobInfo.getQueryType())
        .setSpace(templateJobInfo.getSpace())
        .setUser(templateJobInfo.getUser())
        .setStartTime(start)
        .setFinishTime(end)
        .setFailureInfo(failureInfo)
        .setResourceSchedulingInfo(new ResourceSchedulingInfo().setResourceSchedulingStart(schedulingStart).setResourceSchedulingEnd(schedulingEnd))
        .setDatasetPathList(templateJobInfo.getDatasetPathList());
  }

  private void checkAttemptDetail(AttemptDetailsUI attemptDetails, JobId jobId, int attemptNum, JobState state, String reason) {
    assertEquals("/profiles/" + jobId.getId() + "?attempt=" + attemptNum, attemptDetails.getProfileUrl());
    assertEquals(state, attemptDetails.getResult());
    assertEquals(reason, attemptDetails.getReason());
  }

  @Test
  public void testExplain() throws Exception {
    final SqlQuery query = getQueryFromSQL("EXPLAIN PLAN FOR SELECT * FROM sys.version");
    submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());
  }

  @Test
  public void testAlterOption() throws Exception {
    final SqlQuery query = getQueryFromSQL("alter session set \"planner.enable_multiphase_agg\"=true");
    submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());
  }

  @Test
  public void testAliasedQuery() throws Exception {
    final SqlQuery query = getQueryFromSQL("SHOW SCHEMAS");
    submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());
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
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    ctas = getQueryFromSQL("CREATE OR REPLACE VIEW ctasSpace.ctastest AS select * from (VALUES (2))");
    submitJobAndWaitUntilCompletion(
      JobRequest.newBuilder()
        .setSqlQuery(ctas)
        .setQueryType(QueryType.UI_RUN)
        .build()
    );

    newNamespaceService().deleteSpace(namespaceKey, newNamespaceService().getSpace(namespaceKey).getTag());
  }

  /**
   * This test verifies that metadata is available after listener registration and ExternalListenerManager#metadataAvailable
   * is called within attemptObserver
   * @throws Exception
   */
  @Test
  public void testMetadataAwaitingValidQuery() throws Exception {
    final JobId jobId = submitAndWaitUntilSubmitted(
      JobRequest.newBuilder().setSqlQuery(getQueryFromSQL("SELECT * FROM (VALUES(1234))")).build()
    );
    JobDataClientUtils.waitForBatchSchema(jobsService, jobId);
    com.dremio.service.job.JobDetails jobDetails = jobsService.getJobDetails(JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertEquals("batch schema is not empty after awaiting",true, JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getBatchSchema() != null);
    JobDataClientUtils.waitForFinalState(jobsService, jobId);
  }

  @Test
  public void testMetadataAwaitingInvalidQuery() throws Exception {
    final JobId jobId = submitAndWaitUntilSubmitted(
      JobRequest.newBuilder().setSqlQuery(getQueryFromSQL("SELECT * FROM_1 (VALUES(1234))")).build()
    );
    try {
      JobDataClientUtils.waitForFinalState(jobsService, jobId);
    } catch (Exception e) {
      assertEquals(RuntimeException.class, e.getClass());
    }
    com.dremio.service.job.JobDetails jobDetails = jobsService.getJobDetails(JobDetailsRequest.newBuilder().setJobId(JobsProtoUtil.toBuf(jobId)).build());
    assertEquals("batch schema should be empty for invalid query",true, JobsProtoUtil.getLastAttempt(jobDetails).getInfo().getBatchSchema() == null);
  }

  @Test
  public void testExceptionPropagation() throws Exception {
    final JobSubmittedListener jobSubmittedListener = new JobSubmittedListener();
    final CompletionListener completionListener = new CompletionListener();
    final String testKey = TestingFunctionHelper.newKey(TestJobService::failFunction);

    final JobRequest request = JobRequest.newBuilder()
      .setSqlQuery(new SqlQuery(String.format("SELECT WAIT(key, 5) FROM (VALUES('%s')) tbl(key)", testKey), null, DEFAULT_USERNAME))
      .build();
    final JobId jobId = jobsService.submitJob(toSubmitJobRequest(request), new MultiJobStatusListener(completionListener, jobSubmittedListener));
    jobSubmittedListener.await();

    // Try to get job data while job is still running. The getJobData call implicitly waits for the Job to complete.
    // We expect the exception to be propagated through Arrow Flight and automatically converted to UserException
    final Throwable[] throwable = new Throwable[1];
    Thread t1 = new Thread(new Runnable() {
      @Override
      public void run() {
        try (JobDataFragment jobDataFragment = JobDataClientUtils.getJobData(
          jobsService, l(BufferAllocator.class), jobId, 0, 1)) {
          throwable[0] = new AssertionError("Job data call should not have succeeded");
        } catch (Exception e) {
          throwable[0] = e;
        }
      }
    });
    t1.start();

    // release testLatch so the job can fail
    TestingFunctionHelper.trigger(testKey);
    t1.join();

    // check that the exception is from the failed job
    try {
      throw throwable[0];
    } catch (UserException uex) {
      assertEquals("expected failure", uex.getOriginalMessage());
    } catch (Throwable t) {
      throw new AssertionError(String.format(
        "Got exception of type %s instead of UserRemoteException", t.getClass().getName()));
    }
  }

  /**
   * Test case of Jira DX-31878. If Job Submission fails before jobId is generated,
   * the job just hangs in getJobID as the exception wasn't getting propagated up the chain.
   */

  @Test
  public void testJobSubmitFailure() throws Exception {
    //Submit 4 queries.
    setSystemOption(MAX_FOREMEN_PER_COORDINATOR.getOptionName(), "4");
    String controls = Controls.newBuilder()
      .addPause(DremioVolcanoPlanner.class, INJECTOR_DURING_PLANNING_PAUSE)
      .addPause(DremioHepPlanner.class, INJECTOR_DURING_PLANNING_PAUSE)
      .build();
    injectPauses(controls);
    UserBitShared.ExternalId externalId[] = new UserBitShared.ExternalId[4];
    JobId jobID[] = new JobId[4];
    for (int i = 0; i < 4; i++) {
      final String query = String.format("Select 1");
      jobID[i] = submitAndWaitUntilSubmitted(
        JobRequest.newBuilder()
          .setSqlQuery(new SqlQuery(query, com.dremio.dac.server.test.SampleDataPopulator.DEFAULT_USER_NAME))
          .setQueryType(com.dremio.service.job.proto.QueryType.UI_INTERNAL_RUN)
          .setDatasetPath(com.dremio.dac.explore.model.DatasetPath.NONE.toNamespaceKey())
          .build());
      externalId[i] = ExternalIdHelper.toExternal(QueryIdHelper.getQueryIdFromString(jobID[i].getId()));
    }
    SqlQuery query = getQueryFromSQL("SELECT 1");
    try {
      submitJobAndWaitUntilCompletion(JobRequest.newBuilder().setSqlQuery(query).build());
      fail("job should fail");
    } catch (Exception e) {
      // expected
    }
    for (int i = 0; i< 4; i++) {
      foremenWorkManager.resume(externalId[i]);
      JobDataClientUtils.waitForFinalState(jobsService, jobID[i]);
    }
  }

  /**
   * Factory for providing latches and runnables to Functions
   */
  public static class TestingFunctionHelper {
    private static final Map<String, CountDownLatch> latches = new ConcurrentHashMap<>();
    private static final Map<String, Runnable> runnables = new ConcurrentHashMap<>();

    /**
     * Get a new key and register a new latch and provided runnable to it
     */
    public static String newKey(Runnable runnable) {
      final String key = randomUUID().toString();
      final CountDownLatch latch = new CountDownLatch(1);
      latches.put(key, latch);
      runnables.put(key, runnable);
      return key;
    }

    /**
     * For a given key, wait on its latch, then run its runnable
     */
    public static void tryRun(String key, long timeout, TimeUnit unit) {
      final CountDownLatch latch = latches.get(key);
      final Runnable runnable = runnables.get(key);
      if (latch == null) {
        throw new AssertionError("Latch not registered or already used");
      }
      if (runnable == null) {
        throw new AssertionError("Runnable not registered or already used");
      }

      try {
        latch.await(timeout, unit);
      } catch (InterruptedException e) {
        throw new AssertionError("latch timed out");
      }
      runnable.run();
      latches.remove(key);
      runnables.remove(key);
    }

    /**
     * For a given key, count down its latch
     */
    public static void trigger(String key) {
      final CountDownLatch latch = latches.get(key);
      if (latch == null) {
        throw new AssertionError("Latch not registered or already used");
      }
      latch.countDown();
    }
  }
}
