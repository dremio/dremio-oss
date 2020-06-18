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

import static com.dremio.service.users.SystemUser.SYSTEM_USERNAME;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.dremio.dac.explore.model.DatasetPath;
import com.dremio.dac.server.BaseTestServer;
import com.dremio.dac.support.SupportService;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
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
import com.dremio.service.job.QueryType;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobId;
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
  public void testJobSummaryOnMaster() throws InterruptedException {
    testJobSummaryOn(false);
  }

  // submits job on master & gets summary from slave
  @Test
  public void testJobSummaryOnSlave() throws InterruptedException {
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
  private void testJobSummaryOn(boolean submitJobToMaster) throws InterruptedException {
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
}
