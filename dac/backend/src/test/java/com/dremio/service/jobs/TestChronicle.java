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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.dremio.dac.server.BaseTestServer;
import com.dremio.service.job.ChronicleGrpc;
import com.dremio.service.job.ChronicleGrpc.ChronicleBlockingStub;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.JobsServiceGrpc;
import com.dremio.service.job.JobsServiceGrpc.JobsServiceStub;
import com.dremio.service.job.SqlQuery;
import com.dremio.service.job.StoreJobResultRequest;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.VersionedDatasetPath;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobState;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * Tests for Chronicle service
 */
public class TestChronicle extends BaseTestServer {
  private static Server server;
  private static ManagedChannel channel;
  private static JobsServiceStub asyncStub;
  private static ChronicleBlockingStub chronicleStub;

  @BeforeClass
  public static void setUp() throws IOException {
    final String name = InProcessServerBuilder.generateName();
    server = InProcessServerBuilder.forName(name)
      .directExecutor()
      .addService(new JobsServiceAdapter(p(LocalJobsService.class)))
      .addService(new Chronicle(p(LocalJobsService.class)))
      .build();
    server.start();

    channel = InProcessChannelBuilder.forName(name)
      .directExecutor()
      .build();

    asyncStub = JobsServiceGrpc.newStub(channel);
    chronicleStub = ChronicleGrpc.newBlockingStub(channel);
  }

  @AfterClass
  public static void cleanUp() {
    if (server != null) {
      server.shutdown();
    }
    if (channel != null) {
      channel.shutdown();
    }
  }

  @Test
  public void testRecordJobResult() {
    //submit job and wait for completion
    final CompletionListener completionListener = new CompletionListener();
    final JobStatusListenerAdapter adapter = new JobStatusListenerAdapter(new MultiJobStatusListener(completionListener, JobStatusListener.NO_OP));
    final SqlQuery sqlQuery = SqlQuery.newBuilder()
      .setSql("SELECT * FROM (VALUES(1234))")
      .setUsername(DEFAULT_USERNAME)
      .addAllContext(Collections.<String>emptyList())
      .build();
    asyncStub.submitJob(SubmitJobRequest.newBuilder().setSqlQuery(sqlQuery).build(),adapter);
    final JobId jobId = adapter.getJobId();
    completionListener.awaitUnchecked();

    JobSummary jobSummary = chronicleStub.getJobSummary(JobSummaryRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setUserName(DEFAULT_USERNAME)
      .build());

    assertEquals(com.dremio.service.job.JobState.COMPLETED, jobSummary.getJobState());
    assertEquals(com.dremio.exec.proto.UserBitShared.AttemptEvent.State.COMPLETED,
      jobSummary.getStateList(jobSummary.getStateListCount()-1).getState());

    //get last attempt and modify state
    JobAttempt jobAttempt = JobsProtoUtil.getLastAttempt(chronicleStub.getJobDetails(JobDetailsRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setUserName(DEFAULT_USERNAME)
      .build()));
    jobAttempt.setState(JobState.FAILED);

    //update store with modified job attempt
    chronicleStub.storeJobResult(StoreJobResultRequest.newBuilder()
      .setJobState(com.dremio.service.job.JobState.FAILED)
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setAttemptId(jobAttempt.getAttemptId())
      .setEndpoint(JobsProtoUtil.toBuf(jobAttempt.getEndpoint()))
      .setSql(jobAttempt.getInfo().getSql())
      .setRequestType(JobsProtoUtil.toBuf(jobAttempt.getInfo().getRequestType()))
      .setStartTime(jobAttempt.getInfo().getStartTime())
      .setFinishTime(jobAttempt.getInfo().getFinishTime())
      .setQueryType(JobsProtoUtil.toBuf(jobAttempt.getInfo().getQueryType()))
      .setDescription(jobAttempt.getInfo().getDescription())
      .setOriginalCost(jobAttempt.getInfo().getOriginalCost())
      .addAllOutputTable(jobAttempt.getInfo().getOutputTableList())
      .setDataset(VersionedDatasetPath.newBuilder()
        .addAllPath(jobAttempt.getInfo().getDatasetPathList())
        .setVersion(jobAttempt.getInfo().getDatasetVersion())
        .build())
      .addAllStateList(JobsProtoUtil.toStuff2(jobAttempt.getStateListList()))
      .build());

    //retrieve jobSummary
    jobSummary = chronicleStub.getJobSummary(JobSummaryRequest.newBuilder()
      .setJobId(JobsProtoUtil.toBuf(jobId))
      .setUserName(DEFAULT_USERNAME)
      .build());

    //verify jobstate is modified
    assertEquals(com.dremio.service.job.JobState.FAILED, jobSummary.getJobState());
  }
}
