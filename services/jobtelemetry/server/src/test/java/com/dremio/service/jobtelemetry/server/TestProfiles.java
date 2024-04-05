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
package com.dremio.service.jobtelemetry.server;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dremio.common.AutoCloseables;
import com.dremio.common.concurrent.ContextMigratingExecutorService;
import com.dremio.datastore.DatastoreException;
import com.dremio.exec.proto.CoordExecRPC.ExecutorQueryProfile;
import com.dremio.exec.proto.CoordExecRPC.FragmentStatus;
import com.dremio.exec.proto.CoordExecRPC.NodePhaseStatus;
import com.dremio.exec.proto.CoordExecRPC.NodeQueryStatus;
import com.dremio.exec.proto.CoordExecRPC.QueryProgressMetrics;
import com.dremio.exec.proto.CoordinationProtos.NodeEndpoint;
import com.dremio.exec.proto.ExecProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.QueryId;
import com.dremio.exec.proto.UserBitShared.QueryProfile;
import com.dremio.exec.proto.UserBitShared.QueryResult;
import com.dremio.service.Pointer;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsRequest;
import com.dremio.service.jobtelemetry.GetQueryProgressMetricsResponse;
import com.dremio.service.jobtelemetry.JobTelemetryServiceGrpc;
import com.dremio.service.jobtelemetry.PutExecutorProfileRequest;
import com.dremio.service.jobtelemetry.PutPlanningProfileRequest;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.service.jobtelemetry.server.store.LocalMetricsStore;
import com.dremio.service.jobtelemetry.server.store.LocalProfileStore;
import com.dremio.service.jobtelemetry.server.store.MetricsStore;
import com.dremio.service.jobtelemetry.server.store.ProfileStore;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import io.grpc.testing.GrpcCleanupRule;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/** Tests for ProfileService gRPC API. */
public class TestProfiles {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(TestProfiles.class);

  @Rule public final GrpcCleanupRule grpcCleanupRule = new GrpcCleanupRule();

  private MetricsStore metricsStore;
  private ProfileStore profileStore;
  private JobTelemetryServiceGrpc.JobTelemetryServiceBlockingStub server;
  private JobTelemetryServiceGrpc.JobTelemetryServiceStub asyncServer;
  private JobTelemetryServiceImpl profileService;
  private ContextMigratingExecutorService executorService =
      new ContextMigratingExecutorService(Executors.newCachedThreadPool());

  @Before
  public void setUp() throws Exception {
    // start in-memory metrics store
    metricsStore = new LocalMetricsStore();
    metricsStore.start();

    // start in-memory profile store
    profileStore = new LocalProfileStore(TempLegacyKVStoreProviderCreator.create());
    profileStore.start();

    final String serverName = InProcessServerBuilder.generateName();
    profileService =
        new JobTelemetryServiceImpl(
            metricsStore,
            profileStore,
            new GrpcTracerFacade(TracerFacade.INSTANCE),
            false,
            100,
            executorService);

    grpcCleanupRule.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(profileService)
            .build()
            .start());
    server =
        JobTelemetryServiceGrpc.newBlockingStub(
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
    asyncServer =
        JobTelemetryServiceGrpc.newStub(
            grpcCleanupRule.register(
                InProcessChannelBuilder.forName(serverName).directExecutor().build()));
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(metricsStore, profileStore);
  }

  @Test
  public void testGetQueryProfile() throws Exception {
    ProfileSet profileSet = new ProfileSet();

    // publish all three profiles.
    final QueryId queryId = profileSet.queryId;
    server.putQueryPlanningProfile(profileSet.planningProfileRequest);
    server.putExecutorProfile(profileSet.executorQueryProfileRequests.get(0));
    server.putQueryTailProfile(profileSet.tailProfileRequest);

    // query the profile.
    final QueryProfile queryProfile =
        server
            .getQueryProfile(GetQueryProfileRequest.newBuilder().setQueryId(queryId).build())
            .getProfile();

    // validate the merged profile.
    ExecutorQueryProfile expectedExec = profileSet.executorQueryProfileRequests.get(0).getProfile();
    int max =
        expectedExec.getNodeStatus().getPhaseStatusList().stream()
            .mapToInt(x -> x.getMajorFragmentId())
            .max()
            .orElse(-1);

    assertEquals(max + 1, queryProfile.getFragmentProfileList().size());
    for (int i = 0; i <= max; ++i) {
      assertEquals(
          expectedExec.getEndpoint(),
          queryProfile.getFragmentProfile(i).getNodePhaseProfile(0).getEndpoint());
    }

    assertEquals(profileSet.planningProfileRequest.getProfile().getPlan(), queryProfile.getPlan());
    assertEquals(
        profileSet.tailProfileRequest.getProfile().getCancelReason(),
        queryProfile.getCancelReason());
    assertEquals(
        profileSet.tailProfileRequest.getProfile().getErrorNode(), queryProfile.getErrorNode());

    // wait a little for the background write to complete.
    Thread.sleep(10);

    // fetch profile again (should get the full profile this time without merging), and
    // verify it matches.
    final QueryProfile queryProfileRepeat =
        server
            .getQueryProfile(GetQueryProfileRequest.newBuilder().setQueryId(queryId).build())
            .getProfile();
    assertEquals(queryProfile, queryProfileRepeat);
  }

  // multiple executors
  @Test
  public void testProfileWithMultiExecutors() throws Exception {
    int numExecutors = 6;
    ProfileSet profileSet = new ProfileSet(numExecutors);

    // publish all three profiles.
    final QueryId queryId = profileSet.queryId;
    server.putQueryPlanningProfile(profileSet.planningProfileRequest);
    for (int i = 0; i < numExecutors; ++i) {
      server.putExecutorProfile(profileSet.executorQueryProfileRequests.get(i));
    }
    server.putQueryTailProfile(profileSet.tailProfileRequest);

    // query the profile.
    final QueryProfile queryProfile =
        server
            .getQueryProfile(GetQueryProfileRequest.newBuilder().setQueryId(queryId).build())
            .getProfile();

    assertEquals(numExecutors, queryProfile.getNodeProfileCount());
    assertEquals(numExecutors, queryProfile.getFragmentProfile(0).getNodePhaseProfileCount());
    assertEquals(numExecutors, queryProfile.getFragmentProfile(0).getMinorFragmentProfileCount());
  }

  // expect failure if profile doesn't exist.
  @Test
  public void testGetQueryProfileQueryIdNotExists() {
    final QueryId queryId = QueryId.newBuilder().setPart1(99L).setPart1(88L).build();

    assertThatThrownBy(
            () ->
                server.getQueryProfile(
                    GetQueryProfileRequest.newBuilder().setQueryId(queryId).build()))
        .isInstanceOf(io.grpc.StatusRuntimeException.class)
        .hasMessageContaining(
            "Unable to get query profile. Profile not found for the given queryId.");
  }

  @Test
  public void testWithOnlyTailProfile() {
    final ProfileSet profileSet = new ProfileSet();

    // publish only tail profile.
    server.putQueryTailProfile(profileSet.tailProfileRequest);

    QueryProfile queryProfile =
        server
            .getQueryProfile(
                GetQueryProfileRequest.newBuilder().setQueryId(profileSet.queryId).build())
            .getProfile();

    final UserBitShared.QueryProfile expectedMergedProfile =
        profileSet.tailProfileRequest.getProfile().toBuilder()
            .setTotalFragments(0)
            .setFinishedFragments(0)
            .build();
    assertEquals(expectedMergedProfile, queryProfile);
  }

  // fetch the last metric received. terminate streaming request immediately.
  private long getSimpleQueryProgressMetrics(QueryId queryId) throws InterruptedException {
    final CountDownLatch responseLatch = new CountDownLatch(1);
    final Pointer<Long> recordsProcessed = new Pointer<>();

    StreamObserver<GetQueryProgressMetricsResponse> responseObserver =
        new StreamObserver<GetQueryProgressMetricsResponse>() {
          @Override
          public void onNext(GetQueryProgressMetricsResponse metricsResponse) {
            recordsProcessed.value = metricsResponse.getMetrics().getRowsProcessed();
          }

          @Override
          public void onError(Throwable throwable) {
            responseLatch.countDown();
          }

          @Override
          public void onCompleted() {
            responseLatch.countDown();
          }
        };

    // send request, and complete.
    StreamObserver<GetQueryProgressMetricsRequest> requestObserver =
        asyncServer.getQueryProgressMetrics(responseObserver);
    requestObserver.onNext(GetQueryProgressMetricsRequest.newBuilder().setQueryId(queryId).build());
    requestObserver.onCompleted();

    // wait on response completion.
    responseLatch.await();
    return recordsProcessed.value;
  }

  // fetch the last metric received.
  private QueryProgressMetrics getSimpleQueryProgressMetricsUnary(QueryId queryId)
      throws InterruptedException {
    GetQueryProgressMetricsResponse response =
        server.getQueryProgressMetricsUnary(
            GetQueryProgressMetricsRequest.newBuilder().setQueryId(queryId).build());
    return response.getMetrics();
  }

  @Test
  public void testSimpleGetProgressMetrics() throws InterruptedException {
    final int numExecutors = 2;
    final ProfileSet profileSet = new ProfileSet(numExecutors);

    // publish only executor profiles.
    int numRecords = 0;
    for (int i = 0; i < numExecutors; ++i) {
      PutExecutorProfileRequest req = profileSet.executorQueryProfileRequests.get(i);
      server.putExecutorProfile(req);
      numRecords += req.getProfile().getProgress().getRowsProcessed();
    }

    // verify metrics.
    assertEquals(numRecords, getSimpleQueryProgressMetrics(profileSet.queryId));
    assertEquals(
        getSimpleQueryProgressMetricsUnary(profileSet.queryId).getRowsProcessed(),
        getSimpleQueryProgressMetrics(profileSet.queryId));
  }

  @Test
  public void testGetProgressMetrics() throws InterruptedException {
    final int numExecutors = 3;
    final ProfileSet profileSet = new ProfileSet(numExecutors);
    final CountDownLatch responseLatch = new CountDownLatch(1);
    final List<QueryProgressMetrics> metrics = new ArrayList<>();

    StreamObserver<GetQueryProgressMetricsResponse> responseObserver =
        new StreamObserver<GetQueryProgressMetricsResponse>() {
          @Override
          public void onNext(GetQueryProgressMetricsResponse metricsResponse) {
            metrics.add(metricsResponse.getMetrics());
          }

          @Override
          public void onError(Throwable throwable) {
            responseLatch.countDown();
          }

          @Override
          public void onCompleted() {
            responseLatch.countDown();
          }
        };

    // send metrics request
    StreamObserver<GetQueryProgressMetricsRequest> requestObserver =
        asyncServer.getQueryProgressMetrics(responseObserver);
    requestObserver.onNext(
        GetQueryProgressMetricsRequest.newBuilder().setQueryId(profileSet.queryId).build());

    // publish executor profiles.
    long expectedRecords = 0;
    long outputRecords = 0;
    for (int i = 0; i < numExecutors; i++) {
      PutExecutorProfileRequest req = profileSet.executorQueryProfileRequests.get(i);
      server.putExecutorProfile(req);
      expectedRecords += req.getProfile().getProgress().getRowsProcessed();
      outputRecords += req.getProfile().getProgress().getOutputRecords();

      Thread.sleep(100);
    }

    // terminate request.
    requestObserver.onCompleted();

    // wait on response completion.
    responseLatch.await();

    // verify metrics.
    assertTrue(metrics.size() > 1);
    assertEquals(expectedRecords, metrics.get(metrics.size() - 1).getRowsProcessed());
    assertEquals(outputRecords, metrics.get(metrics.size() - 1).getOutputRecords());

    // verify metrics via unary call
    QueryProgressMetrics metrics2 = getSimpleQueryProgressMetricsUnary(profileSet.queryId);
    assertEquals(expectedRecords, metrics2.getRowsProcessed());
    assertEquals(outputRecords, metrics2.getOutputRecords());
  }

  @Test
  public void testDeleteProfile() throws InterruptedException {
    final int numExecutors = 2;
    final ProfileSet profileSet = new ProfileSet(2);

    // publish planning profile.
    server.putQueryPlanningProfile(profileSet.planningProfileRequest);

    // publish only executor profiles.
    long expectedRecords = 0;
    long outputRecords = 0;
    for (int i = 0; i < numExecutors; ++i) {
      PutExecutorProfileRequest req = profileSet.executorQueryProfileRequests.get(i);
      server.putExecutorProfile(req);

      expectedRecords += req.getProfile().getProgress().getRowsProcessed();
      outputRecords += req.getProfile().getProgress().getOutputRecords();
    }

    // verify metrics via streaming call
    long recordsProcessed = getSimpleQueryProgressMetrics(profileSet.queryId);
    assertEquals(expectedRecords, recordsProcessed);

    // verify metrics via unary call
    QueryProgressMetrics metrics = getSimpleQueryProgressMetricsUnary(profileSet.queryId);
    assertEquals(expectedRecords, metrics.getRowsProcessed());
    assertEquals(outputRecords, metrics.getOutputRecords());

    // publish tail profile.
    server.putQueryTailProfile(profileSet.tailProfileRequest);

    // verify profile.
    QueryProfile queryProfile =
        server
            .getQueryProfile(
                GetQueryProfileRequest.newBuilder().setQueryId(profileSet.queryId).build())
            .getProfile();
    do {
      Thread.sleep(5);
    } while (profileService.getNumInprogressWrites() > 0);

    // delete the profile and fetch again.
    server.deleteProfile(DeleteProfileRequest.newBuilder().setQueryId(profileSet.queryId).build());

    // expect empty metrics.
    assertEquals(-1, getSimpleQueryProgressMetrics(profileSet.queryId));
    assertEquals(-1, getSimpleQueryProgressMetricsUnary(profileSet.queryId).getRowsProcessed());
    assertEquals(-1, getSimpleQueryProgressMetricsUnary(profileSet.queryId).getOutputRecords());

    // expect error on GetProfile
    assertThatThrownBy(
            () ->
                server.getQueryProfile(
                    GetQueryProfileRequest.newBuilder().setQueryId(profileSet.queryId).build()))
        .isInstanceOf(io.grpc.StatusRuntimeException.class)
        .hasMessageContaining(
            "Unable to get query profile. Profile not found for the given queryId.");
  }

  @Test
  public void testPutQueryTailProfileWithRetry() throws Exception {
    final ProfileSet profileSet = new ProfileSet();

    ProfileStore mockedProfileStore = mock(ProfileStore.class);
    MetricsStore mockedMetricsStore = mock(MetricsStore.class);

    final JobTelemetryServiceImpl tmpService =
        new JobTelemetryServiceImpl(
            mockedMetricsStore,
            mockedProfileStore,
            mock(GrpcTracerFacade.class),
            true,
            100,
            executorService);

    when(mockedProfileStore.getPlanningProfile(any(QueryId.class)))
        .thenReturn(Optional.of(profileSet.planningProfileRequest.getProfile()));

    when(mockedProfileStore.getTailProfile(any(QueryId.class)))
        .thenReturn(Optional.of(profileSet.tailProfileRequest.getProfile()));

    when(mockedProfileStore.getAllExecutorProfiles((any(QueryId.class))))
        .thenReturn(Stream.of(profileSet.executorQueryProfileRequests.get(0).getProfile()));

    // fail putFullProfile some number of times
    final int attempts = 3;
    final int[] count = {0};
    doAnswer(
            new Answer<Void>() {
              @Override
              public Void answer(InvocationOnMock invocation) throws Throwable {
                ++count[0];
                if (count[0] >= attempts) {
                  return null;
                } else {
                  throw new DatastoreException("remote put failed");
                }
              }
            })
        .when(mockedProfileStore)
        .putFullProfile(any(UserBitShared.QueryId.class), any(QueryProfile.class));

    tmpService.putQueryTailProfile(profileSet.tailProfileRequest, mock(StreamObserver.class));
    // call should succeeded after attempts
    Assert.assertTrue(attempts == count[0]);
    tmpService.close();
  }

  /** Helper class to generate profile requests. */
  private class ProfileSet {
    private final QueryId queryId;
    private final PutPlanningProfileRequest planningProfileRequest;
    private final PutTailProfileRequest tailProfileRequest;
    private final List<PutExecutorProfileRequest> executorQueryProfileRequests = new ArrayList<>();

    ProfileSet() {
      this(1);
    }

    ProfileSet(int numExecutors) {
      final Random random = new Random();
      final String endpointAddr = "190.190.0.";

      queryId = QueryId.newBuilder().setPart1(random.nextInt()).setPart2(random.nextInt()).build();

      for (int i = 0; i < numExecutors; ++i) {
        final NodeEndpoint nodeEndPoint =
            NodeEndpoint.newBuilder()
                .setAddress(endpointAddr + (i / 2))
                .setFabricPort(i % 2)
                .build();

        final QueryProgressMetrics queryProgressMetrics =
            QueryProgressMetrics.newBuilder().setRowsProcessed(6666).setOutputRecords(8888).build();
        List<NodePhaseStatus> nodePhaseStatuses = new ArrayList<>();
        nodePhaseStatuses.add(
            NodePhaseStatus.newBuilder().setMajorFragmentId(0).setMaxMemoryUsed(6).build());
        nodePhaseStatuses.add(
            NodePhaseStatus.newBuilder().setMajorFragmentId(1).setMaxMemoryUsed(77).build());
        final NodeQueryStatus nodeQueryStatus =
            NodeQueryStatus.newBuilder()
                .setMaxMemoryUsed(666666)
                .addAllPhaseStatus(nodePhaseStatuses)
                .build();

        List<FragmentStatus> fragmentStatuses = new ArrayList<>();
        fragmentStatuses.add(
            FragmentStatus.newBuilder()
                .setHandle(ExecProtos.FragmentHandle.newBuilder().setMajorFragmentId(0).build())
                .setProfile(UserBitShared.MinorFragmentProfile.newBuilder().setEndTime(116).build())
                .build());

        ExecutorQueryProfile executorQueryProfile =
            ExecutorQueryProfile.newBuilder()
                .setEndpoint(nodeEndPoint)
                .setQueryId(queryId)
                .setProgress(queryProgressMetrics)
                .setNodeStatus(nodeQueryStatus)
                .addAllFragments(fragmentStatuses)
                .build();

        executorQueryProfileRequests.add(
            PutExecutorProfileRequest.newBuilder().setProfile(executorQueryProfile).build());
      }

      final QueryProfile planningProfile =
          QueryProfile.newBuilder()
              .setPlan("PLAN_VALUE")
              .setQuery("Select * from plan")
              .setState(QueryResult.QueryState.RUNNING)
              .build();
      planningProfileRequest =
          PutPlanningProfileRequest.newBuilder()
              .setQueryId(queryId)
              .setProfile(planningProfile)
              .build();

      final QueryProfile tailProfile =
          QueryProfile.newBuilder()
              .setPlan("PLAN_VALUE")
              .setQuery("Select * from plan")
              .setState(QueryResult.QueryState.CANCELED)
              .setErrorNode("ERROR_NODE")
              .setCancelReason("Cancel tail")
              .build();
      tailProfileRequest =
          PutTailProfileRequest.newBuilder().setQueryId(queryId).setProfile(tailProfile).build();
    }
  }
}
