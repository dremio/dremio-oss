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

import static com.dremio.service.jobs.AbandonJobsHelper.setAbandonedJobsToFailedState;
import static com.dremio.service.jobs.JobsServiceUtil.finalJobStates;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dremio.common.logging.StructuredLogger;
import com.dremio.datastore.api.LegacyIndexedStore;
import com.dremio.datastore.api.LegacyIndexedStore.LegacyFindByCondition;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.beans.AttemptEvent;
import com.dremio.service.job.NodeStatusRequest;
import com.dremio.service.job.NodeStatusResponse;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobInfo;
import com.dremio.service.job.proto.JobResult;
import com.dremio.service.job.proto.JobState;
import com.dremio.service.job.proto.QueryType;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Local job service tests for tasks on startup. */
public class TestLocalJobsServiceStartup {
  private LegacyIndexedStore<JobId, JobResult> jobStore;
  private StructuredLogger<Job> jobResultLogger;
  private RemoteJobServiceForwarder forwarder;
  private Collection<NodeEndpoint> availableCoords;
  private static final String issuingAddress = "issuingAddress";
  private static final com.dremio.exec.proto.beans.NodeEndpoint issuingNodeEndpointBean =
      com.dremio.exec.proto.beans.NodeEndpoint.getDefaultInstance().setAddress(issuingAddress);
  private static final String currentAddress = "currentAddress";
  private static final com.dremio.exec.proto.beans.NodeEndpoint currNodeEndpointBean =
      new com.dremio.exec.proto.beans.NodeEndpoint();

  private List<JobResult> returns;
  private static final NodeEndpoint currentEndpoint;
  private static final NodeEndpoint issuingEndpoint;
  private static final NodeEndpoint restartedIssuerEndpoint;
  private JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub jobTelemetryServiceStub =
      mock(JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub.class);

  static {
    currNodeEndpointBean.setAddress(currentAddress);
    currentEndpoint = NodeEndpoint.newBuilder().setAddress(currentAddress).build();

    issuingEndpoint = NodeEndpoint.newBuilder().setAddress(issuingAddress).build();

    restartedIssuerEndpoint =
        NodeEndpoint.newBuilder().setAddress(issuingAddress).setStartTime(34).build();
  }

  @Before
  public void beforeEach() {
    jobStore = (LegacyIndexedStore<JobId, JobResult>) mock(LegacyIndexedStore.class);

    jobResultLogger = (StructuredLogger<Job>) mock(StructuredLogger.class);

    forwarder = mock(RemoteJobServiceForwarder.class);

    when(jobStore.find(any(LegacyFindByCondition.class)))
        .thenReturn(
            Sets.difference(EnumSet.allOf(JobState.class), finalJobStates).stream()
                .map(input -> newJobResult(input))
                .collect(Collectors.toList()));

    returns = Lists.newLinkedList();
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                returns.add(JobResult.class.cast(invocation.getArguments()[1]));
                return null;
              }
            })
        .when(jobStore)
        .put(any(JobId.class), any(JobResult.class));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void cleanupJobStateOnStartUp() throws Exception {
    availableCoords = issuerRestart();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(99).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "all job states must be final, or handled by the above method", allJobsCleanedUp(returns));

    validateReturns(returns);
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordPresentOnStartup() throws Exception {
    // The issuing coordinator is present, so no jobs are cleaned up on startup
    availableCoords = issuerPresent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(0).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states are final and not issued by the current restarted coordinator",
        noJobsCleanedUp(returns));
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsIssuingCoordRestartOnStartup() throws Exception {
    // The issuing coordinator of some non final state jobs has been restarted,
    // so its jobs are cleaned up
    availableCoords = issuerRestart();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(99).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states are final and issued by the current restarted coordinator, "
            + "and must have failed",
        allJobsCleanedUp(returns));

    validateReturns(returns);
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordPresentRecurrent() throws Exception {
    // The issuing coordinator is present during the cleanup task, so no jobs are cleaned up
    availableCoords = issuerPresent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(0).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states must be final, and jobs issued by a present coordinator, ",
        noJobsCleanedUp(returns));
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordAbsentRecurrent() throws Exception {
    // The issuing coordinator is absent during the cleanup task, so all jobs are cleaned up
    availableCoords = issuerAbsent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenThrow(new RuntimeException());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states must be final, and jobs issued by an absent coordinator, ",
        allJobsCleanedUp(returns));

    validateReturns(returns);
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordAbsentButRpcSucceeds() throws Exception {
    // The issuing coordinator is absent in service set but is actually alive, so jobs are not
    // cleaned
    availableCoords = issuerAbsent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(0).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states must be final, and jobs issued by a present coordinator, ",
        noJobsCleanedUp(returns));
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordAbsentAndRpcSucceedsWithDiffTime() throws Exception {
    // The issuing coordinator is absent in service set but is alive with diff startTime, so jobs
    // are cleaned
    availableCoords = issuerAbsent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenReturn(NodeStatusResponse.newBuilder().setStartTime(99).build());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states must be final, and jobs issued by an absent coordinator, ",
        allJobsCleanedUp(returns));

    validateReturns(returns);
    verifyProfileDeletion(returns.size());
  }

  @Test
  public void cleanupJobsWithIssuingCoordAbsentAndRpcFails() throws Exception {
    // The issuing coordinator is absent during the cleanup task, so all jobs are cleaned up
    availableCoords = issuerAbsent();
    when(forwarder.getNodeStatus(any(NodeEndpoint.class), any(NodeStatusRequest.class)))
        .thenThrow(new RuntimeException());

    setAbandonedJobsToFailedState(
        jobTelemetryServiceStub,
        jobStore,
        availableCoords,
        jobResultLogger,
        forwarder,
        currNodeEndpointBean,
        System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));

    assertTrue(
        "All job states must be final, and jobs issued by an absent coordinator, ",
        allJobsCleanedUp(returns));

    validateReturns(returns);
    verifyProfileDeletion(returns.size());
  }

  /** Return true if all jobs were cleaned up. */
  private boolean allJobsCleanedUp(List<JobResult> returns) {
    return returns.size() + finalJobStates.size() == JobState.values().length;
  }

  /** Return true if no jobs were cleaned up. */
  private boolean noJobsCleanedUp(List<JobResult> returns) {
    return returns.size() == 0;
  }

  /** Return a list of nodes with the issuing endpoint and the current endpoint. */
  private List<NodeEndpoint> issuerPresent() {
    return ImmutableList.of(currentEndpoint, issuingEndpoint);
  }

  /** Return a list of nodes with just the current endpoint. */
  private List<NodeEndpoint> issuerAbsent() {
    return ImmutableList.of(currentEndpoint);
  }

  /** Returns a list of nodes with the issuing endpoint after restart. */
  private List<NodeEndpoint> issuerRestart() {
    return ImmutableList.of(restartedIssuerEndpoint);
  }

  /** Validate returned job results. */
  private void validateReturns(List<JobResult> returns) {
    for (JobResult result : returns) {
      assertTrue(result.getCompleted());
      assertEquals(result.getAttemptsList().get(0).getState(), JobState.FAILED);
      List<AttemptEvent> stateList = result.getAttemptsList().get(0).getStateListList();
      assertEquals(stateList.get(stateList.size() - 1).getState(), AttemptEvent.State.FAILED);
      assertTrue(
          result
              .getAttemptsList()
              .get(0)
              .getInfo()
              .getFailureInfo()
              .contains("Query failed as Dremio was restarted"));
    }
  }

  /** Verify profile deletion */
  private void verifyProfileDeletion(int numOfInvocation) {
    verify(jobTelemetryServiceStub, times(numOfInvocation)).deleteProfile(any());
  }

  private static Entry<JobId, JobResult> newJobResult(final JobState jobState) {
    return new Entry<JobId, JobResult>() {

      private final JobId id = new JobId(UUID.randomUUID().toString());

      private final JobResult jobResult =
          new JobResult()
              .setAttemptsList(
                  Lists.newArrayList(
                      new JobAttempt()
                          .setAttemptId(UUID.randomUUID().toString())
                          .setInfo(new JobInfo(id, "sql", "dataset-version", QueryType.UI_RUN))
                          .setState(jobState)
                          .setStateListList(
                              new ArrayList<>(
                                  Collections.singleton(
                                      JobsServiceUtil.createAttemptEvent(
                                          JobsServiceUtil.jobStatusToAttemptStatus(jobState),
                                          System.currentTimeMillis()))))
                          .setEndpoint(issuingNodeEndpointBean)));

      @Override
      public JobId getKey() {
        return id;
      }

      @Override
      public JobResult getValue() {
        return jobResult;
      }

      @Override
      public JobResult setValue(JobResult value) {
        throw new UnsupportedOperationException();
      }
    };
  }
}
