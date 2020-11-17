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

import static com.dremio.BaseTestQuery.getFile;
import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.BaseTestQuery;
import com.dremio.QueryTestUtil;
import com.dremio.common.exceptions.UserRemoteException;
import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.client.DremioClient;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.testing.Controls;
import com.dremio.exec.testing.ControlsInjectionUtil;
import com.dremio.exec.work.foreman.AttemptManager;
import com.dremio.sabot.rpc.user.AwaitableUserResultsListener;
import com.dremio.sabot.rpc.user.UserServer;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.conduit.server.ConduitServer;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SearchJobsRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.io.Resources;

import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;


/**
 * starts master & slave co-ordinators with jobservices on both.
 */
public class TestJobServiceWithMultiNodeSetup extends BaseTestServer{
  @ClassRule
  public static final TemporaryFolder temp = new TemporaryFolder();

  private final DatasetPath ds1 = new DatasetPath("s.ds1");
  private static String query1;

  @BeforeClass // same signature to shadow parent's #init
  public static void init() throws Exception {
    // set the log path so we can read logs and confirm that is working.
    final File jsonFolder = temp.newFolder("json");
    jsonFolder.mkdir();
    Files.copy(new File(Resources.getResource("support/server.json").getPath()), new File(jsonFolder, "server.json"));
    System.setProperty(SupportService.DREMIO_LOG_PATH_PROPERTY, temp.getRoot().toString());
    System.setProperty("dremio_multinode", "true");

    // now start server.
    BaseTestServer.init(true);
    populateInitialData();
    query1 = getFile("tpch_quoted.sql");
    LocalJobsService localJobsService = getMasterDremioDaemon().getBindingProvider().lookup(LocalJobsService.class);
    localJobsService.getLocalAbandonedJobsHandler().reschedule(100);
    localJobsService = getCurrentDremioDaemon().getBindingProvider().lookup(LocalJobsService.class);
    localJobsService.getLocalAbandonedJobsHandler().reschedule(100);
  }

  private ManagedChannel getChannelToCoord(boolean master) {
    ConduitServer server = null;
    ConduitProvider conduitProvider = null;
    if (master) {
      server = getMasterDremioDaemon().getBindingProvider().lookup(ConduitServer.class);
      conduitProvider = getMasterDremioDaemon().getBindingProvider().lookup(ConduitProvider.class);
    } else {
      server = getCurrentDremioDaemon().getBindingProvider().lookup(ConduitServer.class);
      conduitProvider = getCurrentDremioDaemon().getBindingProvider().lookup(ConduitProvider.class);
    }

    final int port = server.getPort();
    final CoordinationProtos.NodeEndpoint target = CoordinationProtos.NodeEndpoint.newBuilder()
      .setAddress("127.0.0.1")
      .setConduitPort(port)
      .build();

    final ManagedChannel channel = conduitProvider.getOrCreateChannel(target);
    return channel;
  }

  private DremioClient getDremioClient(boolean master) throws Exception {
    DremioClient dremioClient = new DremioClient(true);
    if (master) {
      final UserServer server = getMasterDremioDaemon().getBindingProvider().lookup(UserServer.class);
      dremioClient.connect(new Properties(){{
        put("direct", "localhost:" + server.getPort());
        put("user", "dremio");
        put("password", "dremio123");
      }});
    } else {
      final UserServer server = getCurrentDremioDaemon().getBindingProvider().lookup(UserServer.class);
      dremioClient.connect(new Properties(){{
        put("direct", "localhost:" + server.getPort());
        put("user", "dremio");
        put("password", "dremio123");
      }});
    }

    return dremioClient;
  }

  private JobsServiceGrpc.JobsServiceStub getSlaveJobsServiceStub() {
    final ManagedChannel channel = getChannelToCoord(false);
    final JobsServiceGrpc.JobsServiceStub stub = JobsServiceGrpc.newStub(channel);
    return stub;
  }

  private JobsServiceGrpc.JobsServiceStub getMasterJobsServiceStub() {
    final ManagedChannel channel = getChannelToCoord(true);
    final JobsServiceGrpc.JobsServiceStub stub = JobsServiceGrpc.newStub(channel);
    return stub;
  }

  private ChronicleGrpc.ChronicleBlockingStub getSlaveChronicleStub() {
    final ManagedChannel channel  = getChannelToCoord(false);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return stub;
  }

  private ChronicleGrpc.ChronicleBlockingStub getMasterChronicleStub() {
    final ManagedChannel channel  = getChannelToCoord(true);
    final ChronicleGrpc.ChronicleBlockingStub stub = ChronicleGrpc.newBlockingStub(channel);
    return stub;
  }

  //submit the job on slave & pull profile from master
  @Test
  public void testGetProfileOnMaster() throws InterruptedException {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = getSlaveJobsServiceStub();
    ChronicleGrpc.ChronicleBlockingStub jobDetailsStub = getMasterChronicleStub();
    testGetProfile(stubToSubmit, jobDetailsStub);
  }

  //submit the job on master & pull profile from slave
  @Test
  public void testGetProfileOnSlave() throws InterruptedException {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = getMasterJobsServiceStub();
    ChronicleGrpc.ChronicleBlockingStub jobDetailsStub = getSlaveChronicleStub();
    testGetProfile(stubToSubmit, jobDetailsStub);
  }


  private void testGetProfile(JobsServiceGrpc.JobsServiceStub stubToSubmit, ChronicleGrpc.ChronicleBlockingStub jobDetailsStub) throws InterruptedException {
    // issue job submit
    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final CompletionListener listener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(listener, submittedListener));
    stubToSubmit.submitJob(
      SubmitJobRequest.newBuilder()
        .setSqlQuery(JobsProtoUtil.toBuf(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" ")))
        .setQueryType(QueryType.UI_RUN)
        .setVersionedDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toNamespaceKey().getPathComponents())
          .build())
        .build(),
      adapter);

    // wait for it to be submitted
    submittedListener.await();
    final JobId id = adapter.getJobId();

    final QueryProfileRequest req = QueryProfileRequest.newBuilder().setAttempt(0).setJobId(JobsProtoUtil.toBuf(id)).build();
    //poll for the profile while the query is running
    while(true) {
      final UserBitShared.QueryProfile qp = jobDetailsStub.getProfile(req);
      final UserBitShared.QueryResult.QueryState queryState = qp.getState();
      if (queryState == UserBitShared.QueryResult.QueryState.RUNNING) {
        Assert.assertTrue(qp.getTotalFragments() > 0);
        Assert.assertTrue(qp.getDatasetProfileCount() > 0);
      }
      if (queryState == UserBitShared.QueryResult.QueryState.COMPLETED) {
        Assert.assertTrue(qp.getFinishedFragments() > 0);
        Assert.assertTrue(qp.getFragmentProfileCount() > 0);
        break;
      }
      Thread.sleep(100);
    }
  }

  /**
   * Submits job on the slave & subscribes to events on the master
   */
  @Test
  public void testListenerOnMaster() throws InterruptedException {
    testListenerOn(false);
  }

  /**
   * Submits job on the master & subscribes to events on the slave
   */
  @Test
  public void testListenerOnSlave() throws InterruptedException {
    testListenerOn(true);
  }

  // submits job on slave & gets details from master
  @Test
  public void testJobDetailsOnMaster() throws InterruptedException {
    testJobDetailsOn(false);
  }

  // submits job on master & gets details from slave
  @Test
  public void testJobDetailsOnSlave() throws InterruptedException {
    testJobDetailsOn(true);
  }

  // submits job on slave & gets summary from master
  @Test
  public void testJobSummaryOnMaster() throws Exception {
    testJobSummaryOn(false);
  }

  // submits job on master & gets summary from slave
  @Test
  public void testJobSummaryOnSlave() throws Exception {
    testJobSummaryOn(true);
  }

  /**
   * @param submitJobToMaster: if true, submission is done on master & subscription on slave or vice versa.
   */
  private void testListenerOn(boolean submitJobToMaster) throws InterruptedException {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = null;
    JobsServiceGrpc.JobsServiceStub stubToMonitor = null;
    if (submitJobToMaster) {
      stubToSubmit = getMasterJobsServiceStub();
      stubToMonitor = getSlaveJobsServiceStub();
    } else {
      stubToSubmit = getSlaveJobsServiceStub();
      stubToMonitor = getMasterJobsServiceStub();
    }

    // issue job submit
    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final CompletionListener listener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(listener, submittedListener));
    stubToSubmit.submitJob(
      SubmitJobRequest.newBuilder()
        .setSqlQuery(JobsProtoUtil.toBuf(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 150")))
        .setQueryType(QueryType.UI_RUN)
        .setVersionedDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toNamespaceKey().getPathComponents())
          .build())
        .build(),
      adapter);

    // wait for it to be submitted
    submittedListener.await();
    final JobId id = adapter.getJobId();

    final CountDownLatch queryLatch = new CountDownLatch(1);
    final DummyJobEventListener dummyJobEventListener = new DummyJobEventListener(queryLatch);
    // subscribe to events
    stubToMonitor.subscribeToJobEvents(JobsProtoUtil.toBuf(id), dummyJobEventListener);
    // wait for the query to complete
    queryLatch.await();

    Assert.assertFalse(dummyJobEventListener.withError);
    Assert.assertFalse(dummyJobEventListener.jobEvents.isEmpty());
    final JobEvent event = dummyJobEventListener.jobEvents.get(dummyJobEventListener.jobEvents.size() - 1);
    Assert.assertEquals(event.getFinalJobSummary().getJobState(), JobState.COMPLETED);
  }


  /**
   * @param submitJobToMaster: if true, submission is done on master & details on slave or vice versa.
   */
  private void testJobDetailsOn(boolean submitJobToMaster) throws InterruptedException {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = null;
    ChronicleGrpc.ChronicleBlockingStub jobDetailsStub = null;
    JobsServiceGrpc.JobsServiceStub jobMonitorStub = null;
    if (submitJobToMaster) {
      stubToSubmit = getMasterJobsServiceStub();
      jobDetailsStub = getSlaveChronicleStub();
      jobMonitorStub = getSlaveJobsServiceStub();
    } else {
      stubToSubmit = getSlaveJobsServiceStub();
      jobDetailsStub = getMasterChronicleStub();
      jobMonitorStub = getMasterJobsServiceStub();
    }

    // submit job
    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final CompletionListener listener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(listener, submittedListener));
    stubToSubmit.submitJob(
      SubmitJobRequest.newBuilder()
        .setSqlQuery(JobsProtoUtil.toBuf(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 150")))
        .setQueryType(QueryType.UI_RUN)
        .setVersionedDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toNamespaceKey().getPathComponents())
          .build())
        .build(),
      adapter);

    // wait for it to submit
    submittedListener.await();
    final JobId id = adapter.getJobId();

    // get details from the other node
    final JobDetailsRequest jobDetailsRequest = JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(id))
      .setUserName(SYSTEM_USERNAME)
      .build();

    JobDetails jobDetails = jobDetailsStub.getJobDetails(jobDetailsRequest);
    Assert.assertTrue(jobDetails.getAttemptsCount() >= 1);

    if (!jobDetails.getCompleted()) {
      final CountDownLatch queryLatch = new CountDownLatch(1);
      final DummyJobEventListener dummyJobEventListener = new DummyJobEventListener(queryLatch);
      //  subscribe to events
      jobMonitorStub.subscribeToJobEvents(JobsProtoUtil.toBuf(id), dummyJobEventListener);
      // wait for the query to complete
      queryLatch.await();

      jobDetails = jobDetailsStub.getJobDetails(jobDetailsRequest);
    }

    Assert.assertTrue(jobDetails.getAttempts(jobDetails.getAttemptsCount() - 1).getStats().getOutputRecords() == 100);
  }


  /**
   * @param submitJobToMaster: if true, submission is done on master & summary on slave or vice versa.
   */
  private void testJobSummaryOn(boolean submitJobToMaster) throws Exception {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = null;
    ChronicleGrpc.ChronicleBlockingStub jobSummaryStub = null;
    JobsServiceGrpc.JobsServiceStub jobMonitorStub = null;
    if (submitJobToMaster) {
      stubToSubmit = getMasterJobsServiceStub();
      jobSummaryStub = getSlaveChronicleStub();
      jobMonitorStub = getSlaveJobsServiceStub();
    } else {
      stubToSubmit = getSlaveJobsServiceStub();
      jobSummaryStub = getMasterChronicleStub();
      jobMonitorStub = getMasterJobsServiceStub();
    }

    // submit job
    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final CompletionListener listener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(listener, submittedListener));
    stubToSubmit.submitJob(
      SubmitJobRequest.newBuilder()
        .setSqlQuery(JobsProtoUtil.toBuf(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 150")))
        .setQueryType(QueryType.UI_RUN)
        .setVersionedDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toNamespaceKey().getPathComponents())
          .build())
        .build(),
      adapter);

    // 2. wait for it to be submitted
    submittedListener.await();
    final JobId id = adapter.getJobId();

    final JobSummaryRequest jobSummaryRequest = JobSummaryRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(id))
      .setUserName(SYSTEM_USERNAME)
      .build();

    // 3. wait for it to complete
    listener.await();

    JobSummary summary = jobSummaryStub.getJobSummary(jobSummaryRequest);
    final int finalStateIdx = summary.getStateListCount()-1;
    Assert.assertFalse(summary.getStateList(finalStateIdx).getState() == UserBitShared.AttemptEvent.State.CANCELED);
    Assert.assertFalse(summary.getStateList(finalStateIdx).getState() == UserBitShared.AttemptEvent.State.FAILED);
    Assert.assertTrue(summary.getNumAttempts() >= 1);

    if (summary.getJobState() != JobState.COMPLETED) {

      final CountDownLatch queryLatch = new CountDownLatch(1);
      final DummyJobEventListener dummyJobEventListener = new DummyJobEventListener(queryLatch);
      // subscribe to events
      jobMonitorStub.subscribeToJobEvents(JobsProtoUtil.toBuf(id), dummyJobEventListener);
      // wait for the query to complete
      queryLatch.await();

      summary = jobSummaryStub.getJobSummary(jobSummaryRequest);
    }

    Assert.assertTrue(summary.getJobState() == JobState.COMPLETED);
    Assert.assertEquals(summary.getOutputRecords(), 100);
  }


  /**
   * @param submitJobToMaster: if true, submission is done on master & search on slave or vice versa.
   */
  private void testJobSearchOn(boolean submitJobToMaster) throws InterruptedException {
    JobsServiceGrpc.JobsServiceStub stubToSubmit = null;
    ChronicleGrpc.ChronicleBlockingStub jobSummaryStub = null;
    JobsServiceGrpc.JobsServiceStub jobMonitorStub = null;
    if (submitJobToMaster) {
      stubToSubmit = getMasterJobsServiceStub();
      jobSummaryStub = getSlaveChronicleStub();
      jobMonitorStub = getSlaveJobsServiceStub();
    } else {
      stubToSubmit = getSlaveJobsServiceStub();
      jobSummaryStub = getMasterChronicleStub();
      jobMonitorStub = getMasterJobsServiceStub();
    }

    // submit job
    final JobSubmittedListener submittedListener = new JobSubmittedListener();
    final CompletionListener listener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(listener, submittedListener));
    stubToSubmit.submitJob(
      SubmitJobRequest.newBuilder()
        .setSqlQuery(JobsProtoUtil.toBuf(getQueryFromSQL("select * from LocalFS1.\"dac-sample1.json\" limit 150")))
        .setQueryType(QueryType.UI_RUN)
        .setVersionedDataset(VersionedDatasetPath.newBuilder()
          .addAllPath(ds1.toNamespaceKey().getPathComponents())
          .build())
        .build(),
      adapter);

    // wait for it to be submitted
    submittedListener.await();
    final JobId id = adapter.getJobId();

    final SearchJobsRequest searchJobsRequest = SearchJobsRequest.newBuilder()
      .setFilterString("(jst==RUNNING,jst==ENGINE_START,jst==QUEUED,jst==SETUP,jst==COMPLETED)")
      .build();

    final CountDownLatch queryLatch = new CountDownLatch(1);
    final DummyJobEventListener dummyJobEventListener = new DummyJobEventListener(queryLatch);
    jobMonitorStub.subscribeToJobEvents(JobsProtoUtil.toBuf(id), dummyJobEventListener);

    boolean found = false;
    while (queryLatch.getCount() > 0) {
      // search as the job progresses
      final List<JobSummary> jobSummaries = ImmutableList.copyOf(jobSummaryStub.searchJobs(searchJobsRequest));
      if (jobSummaries.size() > 0) {
        for (JobSummary summary : jobSummaries) {
          if (JobsProtoUtil.toStuff(summary.getJobId()).equals(id)) {
            found = true;
          }
        }
      }
      Thread.sleep(100);
    }

    if (!found) {
      // search once more, in case the query finished very quickly
      final List<JobSummary> jobSummaries = ImmutableList.copyOf(jobSummaryStub.searchJobs(searchJobsRequest));
      for (JobSummary summary : jobSummaries) {
        if (JobsProtoUtil.toStuff(summary.getJobId()).equals(id)) {
          found = true;
        }
      }
    }

    Assert.assertTrue("Job id was not found", found);
  }

  @Test
  public void testJobSearchOnSlave() throws Exception {
    testJobSearchOn(true);
  }

  @Test
  public void testJobSearchOnMaster() throws Exception {
    testJobSearchOn(false);
  }


  // helper class, to wait for the job submission
  private class DummyJobStatusListener implements JobStatusListener {
    private final CountDownLatch latch;

    public DummyJobStatusListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void jobSubmitted() {
      latch.countDown();
    }
  }

  // helper class to collect job events
  private class DummyJobEventListener implements StreamObserver<JobEvent> {
    private boolean withError = false;
    private Throwable t;
    private final  CountDownLatch latch;
    private List<JobEvent> jobEvents = new ArrayList<>();

    public DummyJobEventListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public void onNext(JobEvent value) {
      jobEvents.add(value);
    }

    @Override
    public void onError(Throwable t) {
      this.t = t;
      withError = true;
      latch.countDown();
    }

    @Override
    public void onCompleted() {
      latch.countDown();
    }
  }

  private void validateFailedJob(JobSummary jobSummary) throws Exception {
    Assert.assertSame(jobSummary.getJobState(), JobState.FAILED);
    Assert.assertNotNull(jobSummary.getFailureInfo());
    Assert.assertTrue(jobSummary.getFailureInfo().contains("Query failed due to kvstore or network errors. Details and profile information for this job may be partial or missing."));
  }

  private void injectProfileException(String controls, String expectedDesc, boolean master) throws Exception {
    JobId jobId = null;
    try {
      DremioClient dremioClient = getDremioClient(master);
      ControlsInjectionUtil.setControls(dremioClient, controls);
      BaseTestQuery.QueryIdCapturingListener capturingListener = new BaseTestQuery.QueryIdCapturingListener();
      final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
      QueryTestUtil.testWithListener(dremioClient, UserBitShared.QueryType.SQL, query1, listener);

      // wait till we have a queryId
      UserBitShared.QueryId queryId;
      while ((queryId = capturingListener.getQueryId()) == null) {
        Thread.sleep(10);
      }
      jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

      listener.await();
    } catch (UserRemoteException e) {
      assertTrue(e.getMessage().contains(expectedDesc));
    }

    Assert.assertNotNull(jobId);

    JobSummary jobSummary = getMasterChronicleStub().getJobSummary(
      JobSummaryRequest.newBuilder()
        .setJobId(JobsProtoUtil.toBuf(jobId))
        .setUserName(DEFAULT_USERNAME)
        .build());

    validateFailedJob(jobSummary);
  }

  @Test
  public void testTailProfileFailedStateMaster() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_TAIL_PROFLE_ERROR,
        RuntimeException.class)
      .build();

    injectProfileException(controls, AttemptManager.INJECTOR_TAIL_PROFLE_ERROR, true);
  }

  @Test
  public void testGetFullProfileFailedStateSlave() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(AttemptManager.class, AttemptManager.INJECTOR_GET_FULL_PROFLE_ERROR,
        RuntimeException.class)
      .build();

    injectProfileException(controls, AttemptManager.INJECTOR_GET_FULL_PROFLE_ERROR, false);
  }

  private void injectAttemptCompletionException(String controls, boolean master) throws Exception {
    JobId jobId = null;
    DremioClient dremioClient = getDremioClient(master);
    ControlsInjectionUtil.setControls(dremioClient, controls);
    BaseTestQuery.QueryIdCapturingListener capturingListener = new BaseTestQuery.QueryIdCapturingListener();
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
    QueryTestUtil.testWithListener(dremioClient, UserBitShared.QueryType.SQL, query1, listener);

    // wait till we have a queryId
    UserBitShared.QueryId queryId;
    while ((queryId = capturingListener.getQueryId()) == null) {
      Thread.sleep(10);
    }
    jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

    listener.await();

    Assert.assertNotNull(jobId);

    while (true) {
      JobSummary jobSummary = getMasterChronicleStub().getJobSummary(
        JobSummaryRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(jobId))
          .setUserName(DEFAULT_USERNAME)
          .build());
      if(jobSummary.getJobState() == JobState.FAILED) {
        validateFailedJob(jobSummary);
        break;
      }
      Thread.sleep(50);
    }
  }

  @Test
  public void testAttemptCompletionFailedStateMaster() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_ERROR,
        IOException.class)
      .build();

    injectAttemptCompletionException(controls, true);
  }

  @Test
  public void testAttemptCompletionFailedStateSlave() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_ERROR,
        IOException.class)
      .build();

    injectAttemptCompletionException(controls, false);
  }

  private void submitQuery(boolean master, String controls, CompletableFuture<JobId> future) throws Exception {
    JobId jobId = null;
    DremioClient dremioClient = getDremioClient(master);
    ControlsInjectionUtil.setControls(dremioClient, controls);
    BaseTestQuery.QueryIdCapturingListener capturingListener = new BaseTestQuery.QueryIdCapturingListener();
    final AwaitableUserResultsListener listener = new AwaitableUserResultsListener(capturingListener);
    QueryTestUtil.testWithListener(dremioClient, UserBitShared.QueryType.SQL, query1, listener);

    // wait till we have a queryId
    UserBitShared.QueryId queryId;
    while ((queryId = capturingListener.getQueryId()) == null) {
      Thread.sleep(10);
    }
    jobId = new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());

    listener.await();

    Assert.assertNotNull(jobId);

    future.complete(jobId);
  }

  private void injectAttemptCompletionException(String controls) throws Exception {
    // submit job on master
    CompletableFuture<JobId> masterJobFuture = new CompletableFuture<>();
    CompletableFuture<JobId> slaveJobFuture = new CompletableFuture<>();
    CompletableFuture.runAsync(()-> {
        try {
          submitQuery(true, controls, masterJobFuture);
        } catch (Exception e) {
        }
      }
    );

    CompletableFuture.runAsync(()-> {
        try {
          submitQuery(false, controls, slaveJobFuture);
        } catch (Exception e) {
        }
      }
    );

    JobId masterJobId = masterJobFuture.get();
    JobId slaveJobId = slaveJobFuture.get();

    while (true) {
      JobSummary jobSummary = getMasterChronicleStub().getJobSummary(
        JobSummaryRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(masterJobId))
          .setUserName(DEFAULT_USERNAME)
          .build());
      if(jobSummary.getJobState() == JobState.FAILED) {
        validateFailedJob(jobSummary);
        break;
      }
      Thread.sleep(50);
    }

    while (true) {
      JobSummary jobSummary = getMasterChronicleStub().getJobSummary(
        JobSummaryRequest.newBuilder()
          .setJobId(JobsProtoUtil.toBuf(slaveJobId))
          .setUserName(DEFAULT_USERNAME)
          .build());
      if(jobSummary.getJobState() == JobState.FAILED) {
        validateFailedJob(jobSummary);
        break;
      }
      Thread.sleep(50);
    }
  }

  @Test
  public void testAttemptCompletionFailedStateBoth() throws Exception {
    final String controls = Controls.newBuilder()
      .addException(LocalJobsService.class, LocalJobsService.INJECTOR_ATTEMPT_COMPLETION_ERROR,
        IOException.class)
      .build();

    injectAttemptCompletionException(controls);
  }
}
