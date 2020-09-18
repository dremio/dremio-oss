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

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.dremio.common.AutoCloseables;
import com.dremio.exec.proto.CoordinationProtos;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.exec.proto.UserBitShared.AttemptEvent;
import com.dremio.exec.proto.UserBitShared.AttemptEvent.State;
import com.dremio.service.DirectProvider;
import com.dremio.service.grpc.GrpcChannelBuilderFactory;
import com.dremio.service.grpc.GrpcServerBuilderFactory;
import com.dremio.service.grpc.SimpleGrpcChannelBuilderFactory;
import com.dremio.service.grpc.SimpleGrpcServerBuilderFactory;
import com.dremio.service.jobtelemetry.DeleteProfileRequest;
import com.dremio.service.jobtelemetry.GetQueryProfileRequest;
import com.dremio.service.jobtelemetry.JobTelemetryClient;
import com.dremio.service.jobtelemetry.PutPlanningProfileRequest;
import com.dremio.service.jobtelemetry.PutTailProfileRequest;
import com.dremio.telemetry.utils.GrpcTracerFacade;
import com.dremio.telemetry.utils.TracerFacade;

/**
 * Tests LocalJobTelemetryServer.
 */
public class TestLocalJobTelemetryServer {
  private final GrpcChannelBuilderFactory grpcChannelBuilderFactory =
    new SimpleGrpcChannelBuilderFactory(TracerFacade.INSTANCE);
  private final GrpcServerBuilderFactory grpcServerBuilderFactory =
    new SimpleGrpcServerBuilderFactory(TracerFacade.INSTANCE);
  private final GrpcTracerFacade tracer =
    new GrpcTracerFacade(TracerFacade.INSTANCE);

  private LocalJobTelemetryServer server;
  private JobTelemetryClient client;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    final CoordinationProtos.NodeEndpoint node =
      CoordinationProtos.NodeEndpoint.newBuilder()
      .setFabricPort(30)
      .build();

    server = new LocalJobTelemetryServer(grpcServerBuilderFactory,
      DirectProvider.wrap(TempLegacyKVStoreProviderCreator.create()),
      DirectProvider.wrap(node),
      tracer);
    server.start();

    client = new JobTelemetryClient(grpcChannelBuilderFactory, DirectProvider.wrap(node));
    client.start();
  }

  @After
  public void tearDown() throws Exception {
    AutoCloseables.close(server, client);
  }

  @Test
  public void testFullProfile() throws Exception {
    UserBitShared.QueryId queryId = UserBitShared.QueryId.newBuilder()
      .setPart1(10000)
      .setPart2(10001)
      .build();

    final UserBitShared.QueryProfile planningProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setState(UserBitShared.QueryResult.QueryState.ENQUEUED)
        .addStateList(AttemptEvent.newBuilder()
          .setState(State.QUEUED).setStartTime(20L).build())
        .build();
    client.getBlockingStub().putQueryPlanningProfile(
      PutPlanningProfileRequest.newBuilder()
        .setQueryId(queryId)
        .setProfile(planningProfile)
        .build()
    );

    final UserBitShared.QueryProfile tailProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .addStateList(AttemptEvent.newBuilder()
          .setState(State.CANCELED).setStartTime(20L).build())
        .build();
    client.getBlockingStub().putQueryTailProfile(
      PutTailProfileRequest.newBuilder()
        .setQueryId(queryId)
        .setProfile(tailProfile)
        .build()
    );

    final UserBitShared.QueryProfile expectedMergedProfile =
      UserBitShared.QueryProfile.newBuilder()
        .setPlan("PLAN_VALUE")
        .setQuery("Select * from plan")
        .setErrorNode("ERROR_NODE")
        .setState(UserBitShared.QueryResult.QueryState.CANCELED)
        .addStateList(AttemptEvent.newBuilder()
          .setState(State.CANCELED).setStartTime(20L).build())
        .setTotalFragments(0)
        .setFinishedFragments(0)
        .build();

    assertEquals(expectedMergedProfile,
      client.getBlockingStub().getQueryProfile(
        GetQueryProfileRequest.newBuilder()
          .setQueryId(queryId)
          .build()
      ).getProfile());

    // delete profile.
    client.getBlockingStub().deleteProfile(
      DeleteProfileRequest.newBuilder()
        .setQueryId(queryId)
        .build()
    );

    // verify it's gone.
    thrown.expect(io.grpc.StatusRuntimeException.class);
    thrown.expectMessage("profile not found for the given queryId");
    client.getBlockingStub().getQueryProfile(
      GetQueryProfileRequest.newBuilder()
        .setQueryId(queryId)
        .build()
    );
  }
}
