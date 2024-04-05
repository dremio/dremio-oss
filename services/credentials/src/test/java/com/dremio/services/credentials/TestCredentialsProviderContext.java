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
package com.dremio.services.credentials;

import static com.dremio.services.credentials.CredentialsTestingUtils.getLeaderConfig;
import static org.junit.Assert.assertTrue;

import com.dremio.services.credentials.proto.CredentialsServiceGrpc;
import com.dremio.test.DremioTest;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

public class TestCredentialsProviderContext extends DremioTest {

  @ClassRule public static final GrpcCleanupRule GRPC_CLEANUP_RULE = new GrpcCleanupRule();
  private CredentialsServiceImpl leader;
  private ManagedChannel channelToLeader;

  @Before
  public void before() throws Exception {
    final String serverName = InProcessServerBuilder.generateName();
    final Injector injector =
        Guice.createInjector(
            new CredentialsProviderModule(
                getLeaderConfig(), ImmutableSet.of(TestingCredentialsProvider.class)));
    leader = injector.getInstance(CredentialsServiceImpl.class);

    // Configure a mock gRPC service that uses the leader credentials and marks the resolved secret
    // as remotely resolved
    final CredentialsServiceGrpc.CredentialsServiceImplBase serviceImpl =
        new CredentialsTestingUtils.TestingCredentialsServiceGrpcServer(leader);
    GRPC_CLEANUP_RULE.register(
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(serviceImpl)
            .build()
            .start());
    channelToLeader =
        GRPC_CLEANUP_RULE.register(
            InProcessChannelBuilder.forName(serverName).directExecutor().build());
  }

  @After
  public void after() throws Exception {
    leader.close();
  }

  @Test
  public void testRemoteLookup() throws Exception {
    final CredentialsProviderContext context =
        new CredentialsProviderContextImpl(() -> true, () -> true, () -> channelToLeader);
    final String resolved =
        context.lookupOnLeader(TestingCredentialsProvider.SCHEME + ":testSecret");
    assertTrue(TestingCredentialsProvider.isResolved(resolved));
    assertTrue(CredentialsTestingUtils.isRemoteResolved(resolved));
  }
}
