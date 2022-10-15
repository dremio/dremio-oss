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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.arrow.memory.BufferAllocator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.dremio.common.logging.StructuredLogger;
import com.dremio.datastore.api.LegacyKVStoreProvider;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.store.JobResultsStoreConfig;
import com.dremio.exec.store.sys.accel.AccelerationManager;
import com.dremio.exec.work.protector.ForemenTool;
import com.dremio.exec.work.rpc.CoordTunnelCreator;
import com.dremio.exec.work.user.LocalQueryExecutor;
import com.dremio.io.file.Path;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValidatorListing;
import com.dremio.service.commandpool.CommandPool;
import com.dremio.service.conduit.client.ConduitProvider;
import com.dremio.service.job.JobEvent;
import com.dremio.service.job.JobState;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.SubmitJobRequest;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.namespace.NamespaceService;
import com.dremio.service.scheduler.Cancellable;
import com.dremio.service.scheduler.Schedule;
import com.dremio.service.scheduler.SchedulerService;
import com.dremio.service.usersessions.UserSessionService;

import io.grpc.stub.StreamObserver;

/**
 * Unit tests for {@link LocalJobsService}
 */
@RunWith(MockitoJUnitRunner.class)
public class TestLocalJobsService {
  private LocalJobsService localJobsService;
  private final Set<CoordinationProtos.NodeEndpoint> nodeEndpoints = new HashSet<>();

  @Mock private LegacyKVStoreProvider kvStoreProvider;
  @Mock private BufferAllocator bufferAllocator;
  private JobResultsStoreConfig jobResultsStoreConfig;
  @Mock private JobResultsStore jobResultsStore;
  @Mock private LocalQueryExecutor localQueryExecutor;
  @Mock private CoordTunnelCreator coordTunnelCreator;
  @Mock private ForemenTool foremenTool;
  private CoordinationProtos.NodeEndpoint nodeEndpoint;
  @Mock private NamespaceService namespaceService;
  @Mock private OptionManager optionManager;
  @Mock private AccelerationManager accelerationManager;
  @Mock private SchedulerService schedulerService;
  @Mock private CommandPool commandPool;
  @Mock private JobTelemetryClient jobTelemetryClient;
  @Mock private StructuredLogger<Job> jobResultLogger;
  @Mock private ConduitProvider conduitProvider;
  @Mock private UserSessionService userSessionService;
  @Mock private OptionValidatorListing optionValidatorProvider;

  @Before
  public void setup() {
    localJobsService = createLocalJobsService(true);
  }

  private LocalJobsService createLocalJobsService(boolean isMaster) {
    return new LocalJobsService(
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
      isMaster,
      () -> conduitProvider,
      () -> userSessionService,
      () -> optionValidatorProvider,
      Collections.emptyList()
    );
  }

  @Test
  public void runQueryAsJobSuccess() throws Exception {
    final LocalJobsService spy = spy(localJobsService);
    JobSummary jobSummary = JobSummary.newBuilder().setJobState(JobState.COMPLETED).build();
    JobEvent jobEvent = JobEvent.newBuilder().setFinalJobSummary(jobSummary).build();
    doAnswer((Answer<JobSubmission>) invocationOnMock -> {
      invocationOnMock.getArgument(1, StreamObserver.class).onNext(jobEvent);
      return new JobSubmission().setJobId(new JobId("foo"));
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name());
  }

  @Test
  public void runQueryAsJobCanceled() {
    final LocalJobsService spy = spy(localJobsService);
    JobSummary jobSummary = JobSummary.newBuilder().setJobState(JobState.CANCELED).build();
    JobEvent jobEvent = JobEvent.newBuilder().setFinalJobSummary(jobSummary).build();
    doAnswer((Answer<JobSubmission>) invocationOnMock -> {
      invocationOnMock.getArgument(1, StreamObserver.class).onNext(jobEvent);
      return new JobSubmission().setJobId(new JobId("foo"));
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    assertThatThrownBy(() -> spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name()))
      .isInstanceOf(IllegalStateException.class);
  }

  @Test
  public void runQueryAsJobFailure() {
    final LocalJobsService spy = spy(localJobsService);
    doAnswer((Answer<JobSubmission>) invocationOnMock -> {
      invocationOnMock.getArgument(1, StreamObserver.class).onError(new NumberFormatException());
      return JobSubmission.getDefaultInstance();
    }).when(spy).submitJob(any(SubmitJobRequest.class), any(StreamObserver.class), any(PlanTransformationListener.class));

    assertThatThrownBy(() -> spy.runQueryAsJob("my query", "my_username", METADATA_REFRESH.name()))
      .isInstanceOf(IllegalStateException.class)
      .hasCauseInstanceOf(NumberFormatException.class);
  }

  @Test
  public void scheduleCleanupOnlyWhenMaster() throws IOException, InterruptedException {
    localJobsService = createLocalJobsService(true);
    nodeEndpoint = CoordinationProtos.NodeEndpoint.newBuilder().build();
    jobResultsStoreConfig = new JobResultsStoreConfig("dummy", Path.of("UNKNOWN"), null);
    Cancellable mockTask = mock(Cancellable.class);
    when(mockTask.isDone()).thenReturn(true);
    when(schedulerService.schedule(any(Schedule.class),
      any(Runnable.class))).thenReturn(mockTask);

    localJobsService.start();

    verify(schedulerService, times(5)).schedule(any(Schedule.class), any(Runnable.class));
  }

  @Test
  public void scheduleCleanupOnlyWhenNotMaster() throws IOException, InterruptedException {
    localJobsService = createLocalJobsService(false);
    nodeEndpoint = CoordinationProtos.NodeEndpoint.newBuilder().build();
    jobResultsStoreConfig = new JobResultsStoreConfig("dummy", Path.of("UNKNOWN"), null);
    Cancellable mockTask = mock(Cancellable.class);
    when(mockTask.isDone()).thenReturn(true);
    when(schedulerService.schedule(any(Schedule.class),
      any(Runnable.class))).thenReturn(mockTask);

    localJobsService.start();

    verify(schedulerService).schedule(any(Schedule.class), any(Runnable.class));
  }

}
