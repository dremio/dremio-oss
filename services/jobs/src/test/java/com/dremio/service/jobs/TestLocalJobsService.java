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

import static com.dremio.service.job.proto.QueryType.METADATA_REFRESH;
import static org.hamcrest.CoreMatchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.util.Collection;
import java.util.HashSet;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;

import com.dremio.common.logging.StructuredLogger;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.options.OptionManager;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.scheduler.SchedulerService;

import io.grpc.stub.StreamObserver;

/**
 * Unit tests for {@link LocalJobsService}
 */
public class TestLocalJobsService {
  private LocalJobsService localJobsService;
  @Mock private LegacyKVStoreProvider kvStoreProvider;
  @Mock private BufferAllocator bufferAllocator;
  @Mock private JobResultsStoreConfig jobResultsStoreConfig;
  @Mock private JobResultsStore jobResultsStore;
  @Mock private LocalQueryExecutor localQueryExecutor;
  @Mock private CoordTunnelCreator coordTunnelCreator;
  @Mock private ForemenTool foremenTool;
  @Mock private CoordinationProtos.NodeEndpoint nodeEndpoint;
  private Collection<CoordinationProtos.NodeEndpoint> nodeEndpoints = new HashSet<>();
  @Mock private NamespaceService namespaceService;
  @Mock private OptionManager optionManager;
  @Mock private AccelerationManager accelerationManager;
  @Mock private SchedulerService schedulerService;
  @Mock private CommandPool commandPool;
  @Mock private JobTelemetryClient jobTelemetryClient;
  @Mock private StructuredLogger<Job> jobResultLogger;
  @Mock private ConduitProvider conduitProvider;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    localJobsService = new LocalJobsService(
      () -> kvStoreProvider,
      bufferAllocator,
      () -> jobResultsStoreConfig,
      () -> jobResultsStore,
      () -> localQueryExecutor,
      () -> coordTunnelCreator,
      () -> foremenTool,
      () -> nodeEndpoint,
      () -> nodeEndpoints,
      () -> namespaceService,
      () -> optionManager,
      () -> accelerationManager,
      () -> schedulerService,
      () -> commandPool,
      () -> jobTelemetryClient,
      jobResultLogger,
      true,
      () -> conduitProvider
    );
  }

  @Test
  public void runQueryAsJobSuccess() throws Exception {
    final LocalJobsService spy = spy(localJobsService);
    JobSummary jobSummary = JobSummary.newBuilder().setJobState(JobState.COMPLETED).build();
    JobEvent jobEvent = JobEvent.newBuilder().setFinalJobSummary(jobSummary).build();
    doAnswer((Answer<JobId>) invocationOnMock -> {
      invocationOnMock.getArgumentAt(1, StreamObserver.class).onNext(jobEvent);
      return new JobId("foo");
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name());
  }

  @Test
  public void runQueryAsJobCanceled() throws Exception {
    final LocalJobsService spy = spy(localJobsService);
    JobSummary jobSummary = JobSummary.newBuilder().setJobState(JobState.CANCELED).build();
    JobEvent jobEvent = JobEvent.newBuilder().setFinalJobSummary(jobSummary).build();
    doAnswer((Answer<JobId>) invocationOnMock -> {
      invocationOnMock.getArgumentAt(1, StreamObserver.class).onNext(jobEvent);
      return new JobId("foo");
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    thrown.expect(IllegalStateException.class);
    spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name());
  }

  @Test
  public void runQueryAsJobFailure() throws Exception {
    final LocalJobsService spy = spy(localJobsService);
    doAnswer((Answer<JobId>) invocationOnMock -> {
      invocationOnMock.getArgumentAt(1, StreamObserver.class).onError(new NumberFormatException());
      return null;
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    thrown.expect(IllegalStateException.class);
    thrown.expectCause(isA(NumberFormatException.class));
    spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name());
  }
}
